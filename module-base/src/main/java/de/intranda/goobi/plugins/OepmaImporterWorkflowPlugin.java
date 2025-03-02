package de.intranda.goobi.plugins;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.UUID;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.lang3.StringUtils;
import org.goobi.goobiScript.GoobiScriptManager;
import org.goobi.goobiScript.GoobiScriptResult;
import org.goobi.production.enums.PluginType;
import org.goobi.production.plugin.interfaces.IPushPlugin;
import org.goobi.production.plugin.interfaces.IWorkflowPlugin;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.omnifaces.cdi.PushContext;

import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.helper.Helper;
import de.sub.goobi.helper.StorageProvider;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import net.xeoh.plugins.base.annotations.PluginImplementation;

@PluginImplementation
@Log4j2
public class OepmaImporterWorkflowPlugin implements IWorkflowPlugin, IPushPlugin {

    private static final long serialVersionUID = 355205933445447355L;
    @Getter
    private String title = "intranda_workflow_oepma_importer";
    private PushContext pusher;
    private long lastPush = System.currentTimeMillis();
    @Getter
    private boolean run = false;
    @Getter
    private int progress = -1;
    @Getter
    private int itemCurrent = 0;
    @Getter
    private int itemsTotal = 0;
    @Getter
    private transient Queue<LogMessage> logQueue = new CircularFifoQueue<>(48);
    private int maxRecords = 10000000;
    private transient MultiValuedMap<String, ImportEntry> importEntries = new ArrayListValuedHashMap<>();

    @Override
    public PluginType getType() {
        return PluginType.Workflow;
    }

    @Override
    public String getGui() {
        return "/uii/plugin_workflow_oepma_importer.xhtml";
    }

    /**
     * cancel a running import
     */
    public void cancel() {
        run = false;
    }

    /**
     * method to prepare small import files inside of the input folder
     * 
     * @param importConfiguration
     */
    public void prepareInputFiles() {
        log.info("Start OEPMA Input file generation");
        String importFolder = ConfigPlugins.getPluginConfig(title).getString("importFolder");
        maxRecords = ConfigPlugins.getPluginConfig(title).getInt("maxRecords");
        List<String> usedProcessTitles = new ArrayList<>();
        progress = 0;
        updateLog("Prepare Input Files");

        // run the import in a separate thread to allow a dynamic progress bar
        run = true;
        Runnable runnable = () -> {

            try {
                // read the xml data files first
                readTableAnmelder(importFolder + "Anmelder.xml");
                readTableMaster(importFolder + "Master.xml");
                readTablePrio(importFolder + "Prio.xml");

                updateLog("Start creation of input files");
                int start = 0;
                int end = importEntries.asMap().keySet().size();
                itemsTotal = end - start;
                itemCurrent = start;

                // run through import objects
                for (String key : importEntries.asMap().keySet()) {
                    Thread.sleep(100);
                    if (!run) {
                        break;
                    }

                    try {
                        Collection<ImportEntry> col = importEntries.get(key);
                        ImportEntry ie = (ImportEntry) col.toArray()[0];

                        Document doc = new Document();
                        doc.setRootElement(new Element("import"));

                        String processname = key.replaceAll("[\\W]", "_").trim();
                        if (StringUtils.isBlank(processname) || processname.length() < 2) {
                            processname = UUID.randomUUID().toString();
                        } else if (usedProcessTitles.contains(processname)) {
                            int tempCounter = 1;
                            String tempName = processname + "_" + tempCounter;
                            while (usedProcessTitles.contains(tempName)) {
                                tempCounter++;
                                tempName = processname + "_" + tempCounter;
                            }
                            processname = tempName;
                            usedProcessTitles.add(processname);
                        }
                        updateLog("Create input file: " + processname, 1);

                        doc.getRootElement().addContent(new Element("processname").setText(processname));
                        doc.getRootElement().addContent(new Element("key").setText(ie.getKey()));
                        doc.getRootElement().addContent(new Element("place").setText(ie.getPlace()));
                        doc.getRootElement().addContent(new Element("country").setText(ie.getCountry()));
                        doc.getRootElement().addContent(new Element("date").setText(ie.getDate()));
                        doc.getRootElement().addContent(new Element("title").setText(ie.getTitle()));
                        doc.getRootElement().addContent(new Element("shelfmark").setText(ie.getShelfmark()));
                        doc.getRootElement().addContent(new Element("pdf").setText(ie.getPdf()));
                        doc.getRootElement().addContent(new Element("notes").setText(ie.getNotes()));

                        Element priorities = new Element("priorities");
                        for (ImportEntryPriority iep : ie.getPriorities()) {
                            Element p = new Element("priority");
                            p.addContent(new Element("country").setText(iep.country));
                            p.addContent(new Element("date").setText(iep.date));
                            priorities.addContent(p);
                        }
                        doc.getRootElement().addContent(priorities);

                        Element persons = new Element("persons");
                        for (ImportEntry importEntry : col) {
                            if (StringUtils.isNoneBlank(importEntry.getFullname())) {
                                Element p = new Element("person");
                                if (importEntry.getFullname().contains(" ")) {
                                    String lastname = importEntry.getFullname()
                                            .substring(0,
                                                    importEntry.getFullname().indexOf(" "));
                                    String firstname = importEntry.getFullname()
                                            .substring(importEntry.getFullname().indexOf(" "));
                                    p.addContent(new Element("firstname").setText(firstname));
                                    p.addContent(new Element("lastname").setText(lastname));
                                } else {
                                    p.addContent(new Element("lastname").setText(importEntry.getFullname()));
                                }
                                persons.addContent(p);
                            }
                        }
                        doc.getRootElement().addContent(persons);

                        XMLOutputter xmlOutputter = new XMLOutputter();
                        xmlOutputter.setFormat(Format.getPrettyFormat());
                        File target = new File(importFolder, "input");
                        target = new File(target, processname + ".xml");
                        FileWriter fileWriter = new FileWriter(target);
                        xmlOutputter.output(doc, fileWriter);

                        updateLog("Input file successfully created: " + processname);

                    } catch (Exception e) {
                        log.error("Error while creating the input files", e);
                        updateLog("Error while creating the input filest: " + e.getMessage(), 3);
                        Helper.setFehlerMeldung("Error while creating the input files: " + e.getMessage());
                        pusher.send("error");
                    }

                    // recalculate progress
                    itemCurrent++;
                    progress = 100 * itemCurrent / itemsTotal;
                    updateLog("Processing of record done.");
                }

                // finally last push
                run = false;
                Thread.sleep(2000);
                updateLog("Import completed.");
            } catch (InterruptedException e) {
                Helper.setFehlerMeldung("Error while trying to create the input files: " + e.getMessage());
                log.error("Error while trying to create the input files", e);
                updateLog("Error while trying to create the input files: " + e.getMessage(), 3);
            }

        };
        new Thread(runnable).start();
    }

    /**
     * main method to start the actual import
     * 
     * @param importConfiguration
     */
    public void readInputFiles() {
        log.info("Start OEPMA Import");
        GoobiScriptOepmaImport gs = new GoobiScriptOepmaImport();
        gs.setPluginTitle(title);
        List<GoobiScriptResult> goobiScriptResults = gs.prepare(null, "ÖPMA Import", null);
        GoobiScriptManager gsm = Helper.getBeanByClass(GoobiScriptManager.class);
        gsm.enqueueScripts(goobiScriptResults);
        gsm.startWork();
        updateLog("ÖPMA Import started", 2);
    }

    @Override
    public void setPushContext(PushContext pusher) {
        this.pusher = pusher;
    }

    /**
     * Read the table 'Anmelder' from given xml file
     * 
     * @param filepath
     */
    public void readTableAnmelder(String filepath) {
        updateLog("Start reading the table Anmelder.xml");

        Document document = OepmaHelper.getSAXParsedDocument(filepath);
        List<Element> list = document.getRootElement().getChildren("Anmelder");
        int size = list.size();
        updateLog("Import Anmelder: " + size);
        int counter = 0;

        for (Element e : list) {
            if (counter++ > maxRecords) {
                break;
            }
            updateLog("Importing anmelder with key: " + e.getChildText("Schluessel") + " (" + counter + " of " + size + ")");
            System.out.println("Importing anmelder with key: " + e.getChildText("Schluessel") + " (" + counter + " of " + size + ")");
            ImportEntry ie = new ImportEntry();
            ie.setKey(e.getChildText("Schluessel"));
            ie.setFullname(e.getChildText("Name"));
            ie.setPlace(e.getChildText("NeuOrt"));
            ie.setCountry(e.getChildText("NeuLand"));
            importEntries.put(ie.getKey(), ie);
        }
    }

    /**
     * Read the table 'Master' from given xml file and enrich the generated model
     * 
     * @param filepath
     */
    public void readTableMaster(String filepath) {
        updateLog("Start reading the table Master.xml");
        Document document = OepmaHelper.getSAXParsedDocument(filepath);
        List<Element> list = document.getRootElement().getChildren("Master");
        int size = list.size();
        updateLog("Import Master: " + size);
        int counter = 0;
        for (Element e : list) {
            if (counter++ > maxRecords) {
                break;
            }

            // first get the key
            String myKey = e.getChildText("Schluessel");
            updateLog("Importing master with key: " + myKey + " (" + counter + " of " + size + ")");
            System.out.println("Importing master with key: " + myKey + " (" + counter + " of " + size + ")");

            // now get the previously created element from the hashmap
            if (importEntries.containsKey(myKey)) {
                Collection<ImportEntry> col = importEntries.get(myKey);
                for (ImportEntry ie : col) {
                    addMasterInformation(e, ie);
                }
            } else {
                ImportEntry ie = new ImportEntry();
                ie.setKey(myKey);
                addMasterInformation(e, ie);
                importEntries.put(ie.getKey(), ie);
            }
        }
    }

    /**
     * add information from master table
     * 
     * @param e
     * @param ie
     */
    private void addMasterInformation(Element e, ImportEntry ie) {
        ie.setDate(e.getChildText("ErtDat"));
        ie.setTitle(e.getChildText("TitelNeu"));
        ie.setShelfmark(e.getChildText("AZNeu"));
        ie.setPdf(e.getChildText("PDFDoc"));
        ie.setNotes(e.getChildText("Bemerkung"));
    }

    /**
     * Read the table 'Prio' from given xml file and enrich the generated model
     * 
     * @param filepath
     */
    public void readTablePrio(String filepath) {
        updateLog("Start reading the table Prio.xml");
        Document document = OepmaHelper.getSAXParsedDocument(filepath);
        List<Element> list = document.getRootElement().getChildren("Prio");
        int size = list.size();
        updateLog("Import Prio: " + size);
        int counter = 0;
        for (Element e : list) {
            if (counter++ > maxRecords) {
                break;
            }
            // first get the key and create a priority
            String myKey = e.getChildText("Schluessel");
            updateLog("Importing prio with key: " + myKey + " (" + counter + " of " + size + ")");
            System.out.println("Importing prio with key: " + myKey + " (" + counter + " of " + size + ")");
            ImportEntryPriority iep = new ImportEntryPriority();
            iep.setDate(e.getChildText("Prio-Datum"));
            iep.setCountry(e.getChildText("Prio-Land"));

            // now get the previously created element from the hashmap
            if (importEntries.containsKey(myKey)) {
                Collection<ImportEntry> col = importEntries.get(myKey);
                for (ImportEntry ie : col) {
                    ie.getPriorities().add(iep);
                }
            } else {
                ImportEntry ie = new ImportEntry();
                ie.setKey(myKey);
                ie.getPriorities().add(iep);
                importEntries.put(ie.getKey(), ie);
            }
        }
    }

    /**
     * print content to sysout for debugging
     */
    private void printEntries() {
        int counter = 0;
        for (ImportEntry ie : importEntries.values()) {
            System.out.println("Schluessel: " + ie.getKey());
            System.out.println("Name:       " + ie.getFullname());
            System.out.println("NeuOrt:     " + ie.getPlace());
            System.out.println("NeuLand:    " + ie.getCountry());

            System.out.println("ErtDat:     " + ie.getDate());
            System.out.println("TitelNeu:   " + ie.getTitle());
            System.out.println("AZNeu:      " + ie.getShelfmark());
            System.out.println("PDFDoc:     " + ie.getPdf());
            System.out.println("Bemerkung:  " + ie.getNotes());
            System.out.println("Filename:   " + ie.getFileName());
            System.out.println("File path:  " + ie.getFilePath());

            System.out.println("Prio:       " + ie.getPriorities());

            System.out.println("------------------- " + ++counter + " ---------------------");
        }
    }

    /**
     * Internal class that holds all data that was read from different xml files
     */
    @Data
    public class ImportEntry {
        // table Anmelder
        private String key;
        private String fullname;
        private String place;
        private String country;

        // table Master
        private String date;
        private String title;
        private String shelfmark;
        private String pdf;
        private String notes;

        private String fileName;
        private String filePath;

        // table Prio
        private List<ImportEntryPriority> priorities = new ArrayList<>();
    }

    /**
     * simple method to send status message to gui
     * 
     * @param logmessage
     */
    private void updateLog(String logmessage) {
        updateLog(logmessage, 0);
    }

    /**
     * simple method to send status message with specific level to gui
     * 
     * @param logmessage
     */
    private void updateLog(String logmessage, int level) {
        logQueue.add(new LogMessage(logmessage, level));
        log.debug(logmessage);
        if (pusher != null && System.currentTimeMillis() - lastPush > 500) {
            lastPush = System.currentTimeMillis();
            pusher.send("update");
        }
    }

    /**
     * Internal class that holds priority data
     */
    @Data
    public class ImportEntryPriority {
        private String date;
        private String country;
    }

    @Data
    @AllArgsConstructor
    public class LogMessage {
        private String message;
        private int level = 0;
    }

    public static void main(String[] args) throws IOException {
        OepmaImporterWorkflowPlugin xd = new OepmaImporterWorkflowPlugin();
        //        xd.readTableAnmelder("/opt/digiverso/import/oepma/Anmelder.xml");
        //        xd.readTableMaster("/opt/digiverso/import/oepma/Master.xml", "/opt/digiverso/import/oepma/Scans");
        //        xd.readTablePrio("/opt/digiverso/import/oepma/Prio.xml");

        List<Path> files =
                StorageProvider.getInstance().listFiles(Paths.get("/opt/digiverso/import/oepma", "input").toString(), OepmaHelper.xmlFilter);
        System.out.println(files);
    }

}
