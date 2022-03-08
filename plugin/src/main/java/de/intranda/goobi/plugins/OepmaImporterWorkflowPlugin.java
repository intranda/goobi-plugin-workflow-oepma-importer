package de.intranda.goobi.plugins;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.goobi.beans.Process;
import org.goobi.beans.Step;
import org.goobi.production.enums.PluginType;
import org.goobi.production.plugin.interfaces.IPushPlugin;
import org.goobi.production.plugin.interfaces.IWorkflowPlugin;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.omnifaces.cdi.PushContext;

import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.config.ConfigurationHelper;
import de.sub.goobi.helper.BeanHelper;
import de.sub.goobi.helper.Helper;
import de.sub.goobi.helper.NIOFileUtils;
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
import ugh.dl.MetadataGroup;
import ugh.dl.Person;
import ugh.dl.Prefs;
import ugh.fileformats.mets.MetsMods;

@PluginImplementation
@Log4j2
public class OepmaImporterWorkflowPlugin implements IWorkflowPlugin, IPushPlugin {

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
    private Queue<LogMessage> logQueue = new CircularFifoQueue<LogMessage>(48);
    private String importFolder;
    private String workflowMedia;
    private String workflowNoMedia;
    private String publicationType;
    private int maxRecords = 10000000;

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
    private MultiValuedMap<String, ImportEntry> importEntries = new ArrayListValuedHashMap<>();

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
     * Constructor
     */
    public OepmaImporterWorkflowPlugin(boolean noConfig) {
    }

    /**
     * private method to read main configuration file
     */
    private void readConfiguration() {
        updateLog("Start reading the configuration");

        // read some main configuration
        importFolder = ConfigPlugins.getPluginConfig(title).getString("importFolder");
        workflowMedia = ConfigPlugins.getPluginConfig(title).getString("workflowMedia");
        workflowNoMedia = ConfigPlugins.getPluginConfig(title).getString("workflowNoMedia");
        publicationType = ConfigPlugins.getPluginConfig(title).getString("publicationType");
        maxRecords = ConfigPlugins.getPluginConfig(title).getInt("maxRecords");

        metadataKey = ConfigPlugins.getPluginConfig(title).getString("metadataKey");
        metadataFullname = ConfigPlugins.getPluginConfig(title).getString("metadataFullname");
        metadataPlace = ConfigPlugins.getPluginConfig(title).getString("metadataPlace");
        metadataCountry = ConfigPlugins.getPluginConfig(title).getString("metadataCountry");
        metadataDate = ConfigPlugins.getPluginConfig(title).getString("metadataDate");
        metadataTitle = ConfigPlugins.getPluginConfig(title).getString("metadataTitle");
        metadataShelfmark = ConfigPlugins.getPluginConfig(title).getString("metadataShelfmark");
        metadataPdf = ConfigPlugins.getPluginConfig(title).getString("metadataPdf");
        metadataNotes = ConfigPlugins.getPluginConfig(title).getString("metadataNotes");
        metadataPriority = ConfigPlugins.getPluginConfig(title).getString("metadataPriority");
        metadataPriorityCountry = ConfigPlugins.getPluginConfig(title).getString("metadataPriorityCountry");
        metadataPriorityDate = ConfigPlugins.getPluginConfig(title).getString("metadataPriorityDate");
        metadataFileName = ConfigPlugins.getPluginConfig(title).getString("metadataFileName");
        metadataFilePath = ConfigPlugins.getPluginConfig(title).getString("metadataFilePath");
        metadataCollection = ConfigPlugins.getPluginConfig(title).getString("metadataCollection");

        // write a log into the UI
        updateLog("Configuration successfully read");
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
    public void prepareSmallImportFiles() {
        log.info("Start OEPMA Import");
        List<String> usedProcessTitles = new ArrayList<String>();
        progress = 0;
        BeanHelper bhelp = new BeanHelper();
        updateLog("Start import");

        // run the import in a separate thread to allow a dynamic progress bar
        run = true;
        Runnable runnable = () -> {

            try {
                // read the xml data files first
                readTableAnmelder(importFolder + "Anmelder.xml");
                readTableMaster(importFolder + "Master.xml", importFolder + "Scans");
                readTablePrio(importFolder + "Prio.xml");

                updateLog("Start running through all import files");
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
                        updateLog("Start importing: " + processname, 1);

                        doc.getRootElement().addContent(new Element("processname").setText(processname));
                        doc.getRootElement().addContent(new Element("key").setText(ie.getKey()));
                        doc.getRootElement().addContent(new Element("place").setText(ie.getPlace()));
                        doc.getRootElement().addContent(new Element("country").setText(ie.getCountry()));
                        doc.getRootElement().addContent(new Element("date").setText(ie.getDate()));
                        doc.getRootElement().addContent(new Element("title").setText(ie.getTitle()));
                        doc.getRootElement().addContent(new Element("shelfmark").setText(ie.getShelfmark()));
                        doc.getRootElement().addContent(new Element("pdf").setText(ie.getPdf()));
                        doc.getRootElement().addContent(new Element("notes").setText(ie.getNotes()));
                        doc.getRootElement().addContent(new Element("filename").setText(ie.getFileName()));
                        doc.getRootElement().addContent(new Element("filepath").setText(ie.getFilePath()));

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
                                priorities.addContent(p);
                            }
                        }
                        doc.getRootElement().addContent(persons);

                        XMLOutputter xmlOutputter = new XMLOutputter();
                        xmlOutputter.setFormat(Format.getPrettyFormat());
                        File target = new File(importFolder, "input");
                        target = new File(target, processname + ".xml");
                        FileWriter fileWriter = new FileWriter(target);
                        xmlOutputter.output(doc, fileWriter);

                        updateLog("Process successfully created with ID: " + processname);

                    } catch (Exception e) {
                        log.error("Error while creating a process during the import", e);
                        updateLog("Error while creating a process during the import: " + e.getMessage(), 3);
                        Helper.setFehlerMeldung("Error while creating a process during the import: " + e.getMessage());
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
                Helper.setFehlerMeldung("Error while trying to execute the import: " + e.getMessage());
                log.error("Error while trying to execute the import", e);
                updateLog("Error while trying to execute the import: " + e.getMessage(), 3);
            }

        };
        new Thread(runnable).start();
    }
    
    /**
     * main method to start the actual import
     * 
     * @param importConfiguration
     */
    public void startImport() {
        log.info("Start OEPMA Import");
        progress = 0;
        BeanHelper bhelp = new BeanHelper();
        updateLog("Start import");

        // run the import in a separate thread to allow a dynamic progress bar
        run = true;
        Runnable runnable = () -> {

            try {
                Path input = Paths.get(importFolder, "input");
                List<Path> files = StorageProvider.getInstance().listFiles(input.toString(), xmlFilter);

                updateLog("Start running through all import files");
                int start = 0;
                int end = files.size();
                itemsTotal = end - start;
                itemCurrent = start;

                // run through import objects
                for (Path f : files) {
                    Thread.sleep(100);
                    if (!run) {
                        break;
                    }

                    Document document = getSAXParsedDocument(f.toString());
                    Element root = document.getRootElement();
                    String processname = root.getChildText("processname");
                    
                    updateLog("Start importing: " + processname, 1);

                    try {
                        // get the correct workflow to use
                        boolean hasMedia = StringUtils.isNotBlank(root.getChildText("filepath"));
                        Process template = ProcessManager.getProcessByExactTitle(hasMedia ? workflowMedia : workflowNoMedia);
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

                        Metadata mdFileName = new Metadata(prefs.getMetadataTypeByName(metadataFileName));
                        mdFileName.setValue(root.getChildText("filename"));
                        logical.addMetadata(mdFileName);

                        Metadata mdFilePath = new Metadata(prefs.getMetadataTypeByName(metadataFilePath));
                        mdFilePath.setValue(root.getChildText("filepath"));
                        logical.addMetadata(mdFilePath);

//                        for (ImportEntryPriority iep : ie.getPriorities()) {
//                            MetadataGroup mdGroup = new MetadataGroup(prefs.getMetadataGroupTypeByName(metadataPriority));
//
//                            Metadata mdPriorityCountry = new Metadata(prefs.getMetadataTypeByName(metadataPriorityCountry));
//                            mdPriorityCountry.setValue(iep.getCountry());
//                            mdGroup.addMetadata(mdPriorityCountry);
//
//                            Metadata mdPriorityDate = new Metadata(prefs.getMetadataTypeByName(metadataPriorityDate));
//                            mdPriorityDate.setValue(iep.getDate());
//                            mdGroup.addMetadata(mdPriorityDate);
//
//                            logical.addMetadataGroup(mdGroup);
//                        }
//
//                        for (ImportEntry importEntry : col) {
//                            if (StringUtils.isNoneBlank(importEntry.getFullname())) {
//                                Person p = new Person(prefs.getMetadataTypeByName(metadataFullname));
//                                if (importEntry.getFullname().contains(" ")) {
//                                    String lastname = importEntry.getFullname()
//                                            .substring(0,
//                                                    importEntry.getFullname().indexOf(" "));
//                                    String firstname = importEntry.getFullname()
//                                            .substring(importEntry.getFullname().indexOf(" "));
//                                    p.setFirstname(firstname);
//                                    p.setLastname(lastname);
//                                } else {
//                                    p.setLastname(importEntry.getFullname());
//                                }
//                                logical.addPerson(p);
//                            }
//                        }

                        // save the process
                        Process process = bhelp.createAndSaveNewProcess(template, processname, fileformat);

                        // add some properties
                        bhelp.EigenschaftHinzufuegen(process, "Template", template.getTitel());
                        bhelp.EigenschaftHinzufuegen(process, "TemplateID", "" + template.getId());
                        ProcessManager.saveProcess(process);

                        // if media files are given, import these into the media folder of the process
                        if (StringUtils.isNotBlank(root.getChildText("filepath"))) {
                            updateLog("Start copying media files, as they are available");
                            File pdf = new File(root.getChildText("filepath"));
                            String targetBase = process.getImagesOrigDirectory(false);
                            if (pdf.canRead()) {
                                StorageProvider.getInstance().createDirectories(Paths.get(targetBase));
                                StorageProvider.getInstance()
                                        .copyFile(Paths.get(pdf.getAbsolutePath()),
                                                Paths.get(targetBase, root.getChildText("filename")));
                            }
                        }

                        // start any open automatic tasks for the created process
                        for (Step s : process.getSchritteList()) {
                            if (s.getBearbeitungsstatusEnum().equals(StepStatus.OPEN) && s.isTypAutomatisch()) {
                                ScriptThreadWithoutHibernate myThread = new ScriptThreadWithoutHibernate(s);
                                myThread.startOrPutToQueue();
                            }
                        }
                        updateLog("Process successfully created with ID: " + process.getId());

                    } catch (Exception e) {
                        log.error("Error while creating a process during the import", e);
                        updateLog("Error while creating a process during the import: " + e.getMessage(), 3);
                        Helper.setFehlerMeldung("Error while creating a process during the import: " + e.getMessage());
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
            } catch (Exception e) {
                Helper.setFehlerMeldung("Error while trying to execute the import: " + e.getMessage());
                log.error("Error while trying to execute the import", e);
                updateLog("Error while trying to execute the import: " + e.getMessage(), 3);
            }

        };
        new Thread(runnable).start();
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

        Document document = getSAXParsedDocument(filepath);
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
    public void readTableMaster(String filepath, String images) {
        updateLog("Start reading the table Master.xml");
        Document document = getSAXParsedDocument(filepath);
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
                    addMasterInformation(images, e, ie);
                }
            } else {
                ImportEntry ie = new ImportEntry();
                ie.setKey(myKey);
                addMasterInformation(images, e, ie);
                importEntries.put(ie.getKey(), ie);
            }
        }
    }

    /**
     * add information from master table
     * 
     * @param images
     * @param e
     * @param ie
     */
    private void addMasterInformation(String images, Element e, ImportEntry ie) {
        ie.setDate(e.getChildText("ErtDat"));
        ie.setTitle(e.getChildText("TitelNeu"));
        ie.setShelfmark(e.getChildText("AZNeu"));
        ie.setPdf(e.getChildText("PDFDoc"));
        ie.setNotes(e.getChildText("Bemerkung"));

        if (StringUtils.isNotBlank(ie.getShelfmark())) {
            ie.setFileName(ie.getShelfmark().replace("/", "") + ".pdf");
            // try to find the pdf file in the file system
            String[] extensions = { "pdf" };
            Collection<File> files = FileUtils.listFiles(new File(images), extensions, true);
            for (File file : files) {
                if (file.getName().equals(ie.getFileName())) {
                    ie.setFilePath(file.getAbsolutePath());
                }
            }
        }
    }

    /**
     * Read the table 'Prio' from given xml file and enrich the generated model
     * 
     * @param filepath
     */
    public void readTablePrio(String filepath) {
        updateLog("Start reading the table Prio.xml");
        Document document = getSAXParsedDocument(filepath);
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
     * simple sax parser using JDom2 for the given xml file
     * 
     * @param fileName
     * @return the read jdom document
     */
    private Document getSAXParsedDocument(final String fileName) {
        SAXBuilder builder = new SAXBuilder();
        Document document = null;
        try {
            document = builder.build(fileName);
        } catch (JDOMException | IOException e) {
            e.printStackTrace();
        }
        return document;
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
        private List<ImportEntryPriority> priorities = new ArrayList<ImportEntryPriority>();
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
        OepmaImporterWorkflowPlugin xd = new OepmaImporterWorkflowPlugin(true);
//        xd.readTableAnmelder("/opt/digiverso/import/oepma/Anmelder.xml");
//        xd.readTableMaster("/opt/digiverso/import/oepma/Master.xml", "/opt/digiverso/import/oepma/Scans");
//        xd.readTablePrio("/opt/digiverso/import/oepma/Prio.xml");

        List<Path> files = StorageProvider.getInstance().listFiles(Paths.get("/opt/digiverso/import/oepma", "input").toString(), xmlFilter);
        System.out.println(files);
        
//        xd.createImportXmlFiles("/opt/digiverso/import/oepma/input");
    }

//    /**
//     * Read the table 'Master' from given xml file and enrich the generated model
//     * 
//     * @param filepath
//     * @throws IOException
//     */
//    public void createImportXmlFiles(String filepath) throws IOException {
//        List<String> usedProcessTitles = new ArrayList<String>();
//        int counter = 0;
//        for (ImportEntry ie : importEntries.values()) {
//
//            Document doc = new Document();
//            doc.setRootElement(new Element("import"));
//
//            String processname = ie.getKey().replaceAll("[\\W]", "_").trim();
//            if (StringUtils.isBlank(processname) || processname.length() < 2) {
//                processname = UUID.randomUUID().toString();
//            } else if (usedProcessTitles.contains(processname)) {
//                int tempCounter = 1;
//                String tempName = processname + "_" + tempCounter;
//                while (usedProcessTitles.contains(tempName)) {
//                    tempCounter++;
//                    tempName = processname + "_" + tempCounter;
//                }
//                processname = tempName;
//                usedProcessTitles.add(processname);
//            }
//            doc.getRootElement().addContent(new Element("processname").setText(processname));
//            doc.getRootElement().addContent(new Element("key").setText(ie.getKey()));
//            doc.getRootElement().addContent(new Element("fullname").setText(ie.getFullname()));
//            doc.getRootElement().addContent(new Element("place").setText(ie.getPlace()));
//            doc.getRootElement().addContent(new Element("country").setText(ie.getCountry()));
//            doc.getRootElement().addContent(new Element("date").setText(ie.getDate()));
//            doc.getRootElement().addContent(new Element("title").setText(ie.getTitle()));
//            doc.getRootElement().addContent(new Element("shelfmark").setText(ie.getShelfmark()));
//            doc.getRootElement().addContent(new Element("pdf").setText(ie.getPdf()));
//            doc.getRootElement().addContent(new Element("notes").setText(ie.getNotes()));
//            doc.getRootElement().addContent(new Element("filename").setText(ie.getFileName()));
//            doc.getRootElement().addContent(new Element("filepath").setText(ie.getFilePath()));
//
//            Element priorities = new Element("priorities");
//            for (ImportEntryPriority iep : ie.getPriorities()) {
//                Element p = new Element("priority");
//                p.addContent(new Element("country").setText(iep.country));
//                p.addContent(new Element("date").setText(iep.date));
//                priorities.addContent(p);
//            }
//            doc.getRootElement().addContent(priorities);
//
//            XMLOutputter xmlOutputter = new XMLOutputter();
//            xmlOutputter.setFormat(Format.getPrettyFormat());
//            File target = new File(filepath, counter + ".xml");
//            FileWriter fileWriter = new FileWriter(target);
//            xmlOutputter.output(doc, fileWriter);
//
//            //            System.out.println("Prio:       " + ie.getPriorities());
//
//            System.out.println("------------------- " + ++counter + " ---------------------");
//        }
//    }

    /**
     * Read the table 'Master' from given xml file and enrich the generated model
     * 
     * @param filepath
     * @throws IOException
     */
    public void readTableMasterTitleOnly(String filepath, String images) throws IOException {
        Document document = getSAXParsedDocument(filepath);
        List<Element> list = document.getRootElement().getChildren("Master");
        StringBuilder sb = new StringBuilder();
        List<String> titles = new ArrayList<String>();
        for (Element e : list) {
            String myKey = e.getChildText("TitelNeu");
            sb.append(myKey + "\n");
            titles.add(myKey);
        }
        FileUtils.write(new File("/opt/digiverso/import/titles.txt"), sb.toString());
    }
    
    public static final DirectoryStream.Filter<Path> xmlFilter = new DirectoryStream.Filter<Path>() {
        @Override
        public boolean accept(Path path) {
            return path.getFileName().toString().endsWith(".xml");
        }
    };
}
