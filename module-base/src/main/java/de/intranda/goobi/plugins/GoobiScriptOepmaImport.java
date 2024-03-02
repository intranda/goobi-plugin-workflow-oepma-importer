package de.intranda.goobi.plugins;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.goobi.beans.Process;
import org.goobi.beans.Step;
import org.goobi.goobiScript.AbstractIGoobiScript;
import org.goobi.goobiScript.GoobiScriptResult;
import org.goobi.goobiScript.IGoobiScript;
import org.goobi.production.enums.GoobiScriptResultType;
import org.jdom2.Document;
import org.jdom2.Element;

import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.helper.BeanHelper;
import de.sub.goobi.helper.Helper;
import de.sub.goobi.helper.ScriptThreadWithoutHibernate;
import de.sub.goobi.helper.StorageProvider;
import de.sub.goobi.helper.enums.StepStatus;
import de.sub.goobi.persistence.managers.ProcessManager;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import ugh.dl.DigitalDocument;
import ugh.dl.DocStruct;
import ugh.dl.Fileformat;
import ugh.dl.Metadata;
import ugh.dl.MetadataGroup;
import ugh.dl.Person;
import ugh.dl.Prefs;
import ugh.fileformats.mets.MetsMods;

@Log4j2
public class GoobiScriptOepmaImport extends AbstractIGoobiScript implements IGoobiScript {

	@Setter
	private String pluginTitle;
	
	private String importFolder;
    private String workflowMedia;
    private String workflowNoMedia;
    private String publicationType;
    
    private String metadataKey;
    private String metadataFullname;
    private String metadataPlace;
    private String metadataCountry;
    private String metadataDate;
    private String metadataTitle;
    private String metadataShelfmark;
    private String metadataPdf;
    private String metadataNotes;
    private String metadataPriority;
    private String metadataPriorityCountry;
    private String metadataPriorityDate;
    private String metadataFileName;
    private String metadataFilePath;
    private String metadataCollection;
	
    @Override
    public List<GoobiScriptResult> prepare(List<Integer> processes, String command, Map<String, String> parameters) {
        List<GoobiScriptResult> newList = new ArrayList<>();
        starttime = (int) System.currentTimeMillis() / 1000;
        username = Helper.getLoginBean().getMyBenutzer().getNachVorname();

        importFolder = ConfigPlugins.getPluginConfig(pluginTitle).getString("importFolder");
        Path input = Paths.get(importFolder, "input");
        List<Path> files = StorageProvider.getInstance().listFiles(input.toString(), OepmaHelper.xmlFilter);
        int count = 0;
        for (Path f : files) {
        	Map<String, String> mufParams = new LinkedHashMap<String, String>();
            mufParams.put("filename", f.toString());
            GoobiScriptResult gsr = new GoobiScriptResult(count++, command, mufParams, username, starttime);
            gsr.setCustomGoobiScriptImpl(this);
            gsr.setProcessTitle(f.toFile().getName());
            newList.add(gsr);
        }
        return newList;
    }

    /**
     * private method to read main configuration file
     */
    private void readConfiguration() {
        // read some main configuration
        importFolder = ConfigPlugins.getPluginConfig(pluginTitle).getString("importFolder");
        workflowMedia = ConfigPlugins.getPluginConfig(pluginTitle).getString("workflowMedia");
        workflowNoMedia = ConfigPlugins.getPluginConfig(pluginTitle).getString("workflowNoMedia");
        publicationType = ConfigPlugins.getPluginConfig(pluginTitle).getString("publicationType");
        
        metadataKey = ConfigPlugins.getPluginConfig(pluginTitle).getString("metadataKey");
        metadataFullname = ConfigPlugins.getPluginConfig(pluginTitle).getString("metadataFullname");
        metadataPlace = ConfigPlugins.getPluginConfig(pluginTitle).getString("metadataPlace");
        metadataCountry = ConfigPlugins.getPluginConfig(pluginTitle).getString("metadataCountry");
        metadataDate = ConfigPlugins.getPluginConfig(pluginTitle).getString("metadataDate");
        metadataTitle = ConfigPlugins.getPluginConfig(pluginTitle).getString("metadataTitle");
        metadataShelfmark = ConfigPlugins.getPluginConfig(pluginTitle).getString("metadataShelfmark");
        metadataPdf = ConfigPlugins.getPluginConfig(pluginTitle).getString("metadataPdf");
        metadataNotes = ConfigPlugins.getPluginConfig(pluginTitle).getString("metadataNotes");
        metadataPriority = ConfigPlugins.getPluginConfig(pluginTitle).getString("metadataPriority");
        metadataPriorityCountry = ConfigPlugins.getPluginConfig(pluginTitle).getString("metadataPriorityCountry");
        metadataPriorityDate = ConfigPlugins.getPluginConfig(pluginTitle).getString("metadataPriorityDate");
        metadataFileName = ConfigPlugins.getPluginConfig(pluginTitle).getString("metadataFileName");
        metadataFilePath = ConfigPlugins.getPluginConfig(pluginTitle).getString("metadataFilePath");
        metadataCollection = ConfigPlugins.getPluginConfig(pluginTitle).getString("metadataCollection");
    }
    
    @Override
    public void execute(GoobiScriptResult gsr) {
        // execute all jobs that are still in waiting state
        gsr.updateTimestamp();
        readConfiguration();
        BeanHelper bhelp = new BeanHelper();
        
        Path f = Paths.get(gsr.getParameters().get("filename"));

        Document document = OepmaHelper.getSAXParsedDocument(f.toString());
        Element root = document.getRootElement();
        String processname = root.getChildText("processname");
        
        try {
            // get the correct workflow to use dependent on import file
        	// if media files are given, import these into the media folder of the process
        	File pdfFile = null;
        	String pdfFileName = null;
        	
            if (StringUtils.isNotBlank(root.getChildText("shelfmark"))) {
            	// find out real file name
            	pdfFileName = root.getChildText("shelfmark").replace("/", "") + ".pdf";

                // try to find the pdf file in the file system
                String[] extensions = { "pdf" };
                Collection<File> pfiles = FileUtils.listFiles(new File(importFolder + "Scans"), extensions, true);
                for (File file : pfiles) {
                    if (file.getName().equals(pdfFileName) && file.canRead()) {
                    	log.debug("PDF media file exists");
                        pdfFile = file;                                    
                    }
                }
            }    
        	
        	// select a process template
        	Process template = ProcessManager.getProcessByExactTitle(pdfFile != null ? workflowMedia : workflowNoMedia);
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

            Metadata mdKey = new Metadata(prefs.getMetadataTypeByName(metadataKey));
            mdKey.setValue(root.getChildText("key"));
            logical.addMetadata(mdKey);

            Metadata mdID = new Metadata(prefs.getMetadataTypeByName("CatalogIDDigital"));
            mdID.setValue(processname);
            logical.addMetadata(mdID);

            Metadata mdCol = new Metadata(prefs.getMetadataTypeByName("singleDigCollection"));
            mdCol.setValue(metadataCollection);
            logical.addMetadata(mdCol);

            Metadata mdPlace = new Metadata(prefs.getMetadataTypeByName(metadataPlace));
            mdPlace.setValue(root.getChildText("place"));
            logical.addMetadata(mdPlace);

            Metadata mdCountry = new Metadata(prefs.getMetadataTypeByName(metadataCountry));
            mdCountry.setValue(root.getChildText("country"));
            logical.addMetadata(mdCountry);

            Metadata mdDate = new Metadata(prefs.getMetadataTypeByName(metadataDate));
            mdDate.setValue(root.getChildText("date"));
            logical.addMetadata(mdDate);

            Metadata mdTitle = new Metadata(prefs.getMetadataTypeByName(metadataTitle));
            mdTitle.setValue(root.getChildText("title"));
            logical.addMetadata(mdTitle);

            Metadata mdShelfmark = new Metadata(prefs.getMetadataTypeByName(metadataShelfmark));
            mdShelfmark.setValue(root.getChildText("shelfmark"));
            logical.addMetadata(mdShelfmark);

            Metadata mdPdf = new Metadata(prefs.getMetadataTypeByName(metadataPdf));
            mdPdf.setValue(root.getChildText("pdf"));
            logical.addMetadata(mdPdf);

            Metadata mdNotes = new Metadata(prefs.getMetadataTypeByName(metadataNotes));
            mdNotes.setValue(root.getChildText("notes"));
            logical.addMetadata(mdNotes);

            List<Element> plist = root.getChild("priorities").getChildren("priority");
            for (Element pe : plist) {
                MetadataGroup mdGroup = new MetadataGroup(prefs.getMetadataGroupTypeByName(metadataPriority));

                Metadata mdPriorityCountry = new Metadata(prefs.getMetadataTypeByName(metadataPriorityCountry));
                mdPriorityCountry.setValue(pe.getChildText("country"));
                mdGroup.addMetadata(mdPriorityCountry);

                Metadata mdPriorityDate = new Metadata(prefs.getMetadataTypeByName(metadataPriorityDate));
                mdPriorityDate.setValue(pe.getChildText("date"));
                mdGroup.addMetadata(mdPriorityDate);

                logical.addMetadataGroup(mdGroup);                        
            }

            plist = root.getChild("persons").getChildren("person");
            for (Element pe : plist) {
                Person p = new Person(prefs.getMetadataTypeByName(metadataFullname));
                if (StringUtils.isNotBlank(pe.getChildText("firstname"))) {
                    p.setFirstname(pe.getChildText("firstname"));
                }
                p.setLastname(pe.getChildText("lastname"));
                logical.addPerson(p);        
            }

            if (StringUtils.isNotBlank(pdfFileName)) {
                Metadata mdFileName = new Metadata(prefs.getMetadataTypeByName(metadataFileName));
                mdFileName.setValue(pdfFileName);
                logical.addMetadata(mdFileName);
            } 
            
            if (pdfFile != null) {
            	Metadata mdFilePath = new Metadata(prefs.getMetadataTypeByName(metadataFilePath));
            	mdFilePath.setValue(pdfFile.getAbsolutePath());
            	logical.addMetadata(mdFilePath);
            }
            
            // save the process
            Process process = bhelp.createAndSaveNewProcess(template, processname, fileformat);

            // add some properties
            bhelp.EigenschaftHinzufuegen(process, "Template", template.getTitel());
            bhelp.EigenschaftHinzufuegen(process, "TemplateID", "" + template.getId());
            ProcessManager.saveProcess(process);

            // if media file can be read, import this into the media folder of the process
            if (pdfFile != null) {
            	log.debug("Start copying media file");
                String targetBase = process.getImagesOrigDirectory(false);
                StorageProvider.getInstance().createDirectories(Paths.get(targetBase));
                
                // create a hard link in java
                Files.createLink(Paths.get(targetBase, pdfFile.getName()), pdfFile.toPath());
                
                // copy the file to the target directory
                //StorageProvider.getInstance()
                //   .copyFile(pdfFile.toPath(),
                //    Paths.get(targetBase, pdfFile.getName()));                            
            }

            // start any open automatic tasks for the created process
            for (Step s : process.getSchritteList()) {
                if (s.getBearbeitungsstatusEnum().equals(StepStatus.OPEN) && s.isTypAutomatisch()) {
                    ScriptThreadWithoutHibernate myThread = new ScriptThreadWithoutHibernate(s);
                    myThread.startOrPutToQueue();
                }
            }
            
            // move input file to other location
            Path success = Paths.get(importFolder, "success");
            if (!success.toFile().exists()) {
            	StorageProvider.getInstance().createDirectories(success);
            }
            StorageProvider.getInstance().move(f, Paths.get(success.toString(), f.getFileName().toString()));
            log.debug("Process successfully created with ID: " + process.getId());
            gsr.setResultMessage("Import successfully finished.");
            gsr.setResultType(GoobiScriptResultType.OK);

        } catch (Exception e) {
            log.error("Error while importing the data for process " + f.toString(), e);
            gsr.setResultMessage("Error while importing the data for process: " + e.getMessage());
            gsr.setResultType(GoobiScriptResultType.ERROR);
        }
        gsr.updateTimestamp();
    }

    @Override
    public boolean isVisible() {
        return false;
    }

    @Override
    public String getAction() {
        return "oepmaImport";
    }

}
