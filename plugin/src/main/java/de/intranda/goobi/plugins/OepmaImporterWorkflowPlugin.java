package de.intranda.goobi.plugins;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.goobi.beans.Process;
import org.goobi.beans.Step;
import org.goobi.production.enums.PluginType;
import org.goobi.production.plugin.interfaces.IPushPlugin;
import org.goobi.production.plugin.interfaces.IWorkflowPlugin;
import org.omnifaces.cdi.PushContext;

import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.helper.BeanHelper;
import de.sub.goobi.helper.Helper;
import de.sub.goobi.helper.ScriptThreadWithoutHibernate;
import de.sub.goobi.helper.StorageProvider;
import de.sub.goobi.helper.enums.StepStatus;
import de.sub.goobi.persistence.managers.ProcessManager;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import net.xeoh.plugins.base.annotations.PluginImplementation;
import ugh.dl.DigitalDocument;
import ugh.dl.DocStruct;
import ugh.dl.Fileformat;
import ugh.dl.Metadata;
import ugh.dl.Person;
import ugh.dl.Prefs;
import ugh.fileformats.mets.MetsMods;

@PluginImplementation
@Log4j2
public class OepmaImporterWorkflowPlugin implements IWorkflowPlugin, IPushPlugin {

    @Getter
    private String title = "intranda_workflow_oepma_importer";
    @Getter
    private List<ImportSet> importSets;
    private PushContext pusher;
    @Getter
    private boolean run = false;
    @Getter
    private int progress = -1;
    @Getter
    private int itemCurrent = 0;
    @Getter
    int itemsTotal = 0;
    @Getter
    private Queue<LogMessage> logQueue = new CircularFifoQueue<LogMessage>(48);
    private String importFolder;
    private String workflow;
    private String publicationType;
    
    @Override
    public PluginType getType() {
        return PluginType.Workflow;
    }

    @Override
    public String getGui() {
        return "/uii/plugin_workflow_oepma_importer.xhtml";
    }

    /**
     * Constructor
     */
    public OepmaImporterWorkflowPlugin() {
        log.info("Sample importer workflow plugin started");

        // read important configuration first
        readConfiguration();
    }

    /**
     * private method to read main configuration file
     */
    private void readConfiguration() {
        logQueue.add(new LogMessage("Start reading the configuration", 0));
        
        // read some main configuration
        importFolder = ConfigPlugins.getPluginConfig(title).getString("importFolder");
        workflow = ConfigPlugins.getPluginConfig(title).getString("workflow");
        publicationType = ConfigPlugins.getPluginConfig(title).getString("publicationType");
        
        // read list of mapping configuration
        importSets = new ArrayList<ImportSet>();
        List<HierarchicalConfiguration> mappings = ConfigPlugins.getPluginConfig(title).configurationsAt("importSet");
        for (HierarchicalConfiguration node : mappings) {
            String settitle = node.getString("[@title]", "-");
            String source = node.getString("[@source]", "-");
            String target = node.getString("[@target]", "-");
            boolean person = node.getBoolean("[@person]", false);
            importSets.add(new ImportSet(settitle, source, target, person));
        }
        
        // write a log into the UI
        logQueue.add(new LogMessage("Configuration successfully read", 0));
    }

    /**
     * cancel a running import
     */
    public void cancel() {
        run = false;
    }

    /**
     * main method to start the actual import
     * 
     * @param importConfiguration
     */
    public void startImport(ImportSet importset) {
        log.info("Start OEPMA Import for: " + importset.getTitle());
        progress = 0;
        BeanHelper bhelp = new BeanHelper();
        logQueue.add(new LogMessage("Start import", 0));
        
        // run the import in a separate thread to allow a dynamic progress bar
        run = true;
        Runnable runnable = () -> {
            long lastPush = System.currentTimeMillis();
           
            // read input file
            try {
                logQueue.add(new LogMessage("Run through all import files", 0));
                int start = 0;
                int end = 20;
                itemsTotal = end - start;
                itemCurrent = start;
                
                // run through import files (e.g. from importFolder)
                for (int i = start; i < end; i++) {
                    Thread.sleep(100);
                    if (!run) {
                        break;
                    }

                    // create a process name (here as UUID) and make sure it does not exist yet
                    String processname = processname = UUID.randomUUID().toString();  
                    if (ProcessManager.countProcessTitle(processname, null) > 0) {
                        int tempCounter = 1;
                        String tempName = processname + "_" + tempCounter;
                        while(ProcessManager.countProcessTitle(tempName, null) > 0) {
                            tempCounter++;
                            tempName = processname + "_" + tempCounter;
                        }
                        processname = tempName;
                    }
                    logQueue.add(new LogMessage("Start importing: " + processname, 1));

                    try {
                        // get the correct workflow to use
                        Process template = ProcessManager.getProcessByExactTitle(workflow);
                        Prefs prefs = template.getRegelsatz().getPreferences();
                        Fileformat fileformat = new MetsMods(prefs);
                        DigitalDocument dd = new DigitalDocument();
                        fileformat.setDigitalDocument(dd);

                        // add the physical basics
                        DocStruct physical = dd.createDocStruct(prefs.getDocStrctTypeByName("BoundBook"));
                        dd.setPhysicalDocStruct(physical);
                        Metadata mdForPath = new Metadata(prefs.getMetadataTypeByName("pathimagefiles"));
                        mdForPath.setValue("file:///");
                        physical.addMetadata(mdForPath);

                        // add the logical basics
                        DocStruct logical = dd.createDocStruct(prefs.getDocStrctTypeByName(publicationType));
                        dd.setLogicalDocStruct(logical);

                        // create the metadata fields by reading the config (and get content from the content files of course)
                        for (ImportSet importSet : importSets) {
                            // treat persons different than regular metadata
                            if (importSet.isPerson()) {
                                logQueue.add(new LogMessage("Add person '" + importSet.getTarget() + "' with value '" + importSet.getSource() + "'", 0));
                                Person p = new Person(prefs.getMetadataTypeByName(importSet.getTarget()));
                                String firstname = importSet.getSource().substring(0, importSet.getSource().indexOf(" "));
                                String lastname = importSet.getSource().substring(importSet.getSource().indexOf(" "));
                                p.setFirstname(firstname);
                                p.setLastname(lastname);
                                logical.addPerson(p);       
                            } else {
                                logQueue.add(new LogMessage("Add metadata '" + importSet.getTarget() + "' with value '" + importSet.getSource() + "'", 0));
                                Metadata mdTitle = new Metadata(prefs.getMetadataTypeByName(importSet.getTarget()));
                                mdTitle.setValue(importSet.getSource());
                                logical.addMetadata(mdTitle);
                            }
                        }

                        // save the process
                        Process process = bhelp.createAndSaveNewProcess(template, processname, fileformat);

                        // add some properties
                        bhelp.EigenschaftHinzufuegen(process, "Template", template.getTitel());
                        bhelp.EigenschaftHinzufuegen(process, "TemplateID", "" + template.getId());
                        ProcessManager.saveProcess(process);
                        
                        // if media files are given, import these into the media folder of the process
                        logQueue.add(new LogMessage("Copy Media files", 0));
                        String targetBase = process.getImagesOrigDirectory(false);
                        File pdf = new File(importFolder, "file.pdf");
                        if (pdf.canRead()) {
                            StorageProvider.getInstance().createDirectories(Paths.get(targetBase));
                            StorageProvider.getInstance().copyFile(Paths.get(pdf.getAbsolutePath()), Paths.get(targetBase, "file.pdf"));
                        }

                        // start any open automatic tasks for the created process
                        for (Step s : process.getSchritteList()) {
                            if (s.getBearbeitungsstatusEnum().equals(StepStatus.OPEN) && s.isTypAutomatisch()) {
                                ScriptThreadWithoutHibernate myThread = new ScriptThreadWithoutHibernate(s);
                                myThread.startOrPutToQueue();
                            }
                        }
                        logQueue.add(new LogMessage("Process successfully created with ID: " + process.getId(), 0));

                    } catch (Exception e) {
                        log.error("Error while creating a process during the import", e);
                        Helper.setFehlerMeldung("Error while creating a process during the import: " + e.getMessage());
                        pusher.send("error");
                    }

                    // recalculate progress
                    itemCurrent++;
                    progress = 100 * itemCurrent / itemsTotal;
                    
                    // update progress bar
                    if (pusher != null && System.currentTimeMillis() - lastPush > 500) {
                        lastPush = System.currentTimeMillis();
                        pusher.send("update");
                    }
                }
                
                // finally last push
                run = false;
                Thread.sleep(2000);
                if (pusher != null) {
                    pusher.send("update");
                }
            } catch (InterruptedException e) {
                Helper.setFehlerMeldung("Error while trying to execute the Excel import: " + e.getMessage());
                log.error("Error while trying to execute the Excel import", e);
            }

        };
        new Thread(runnable).start();
    }

    @Override
    public void setPushContext(PushContext pusher) {
        this.pusher = pusher;
    }

    @Data
    @AllArgsConstructor
    public class ImportSet {
        private String title;
        private String source;
        private String target;
        private boolean person;
    }

    @Data
    @AllArgsConstructor
    public class LogMessage {
        private String message;
        private int level = 0;
    }
}
