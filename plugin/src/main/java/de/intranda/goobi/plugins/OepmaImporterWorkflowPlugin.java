package de.intranda.goobi.plugins;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.io.FileUtils;
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
import org.omnifaces.cdi.PushContext;

import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.config.ConfigurationHelper;
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
    private String workflow;
    private String publicationType;
    private int maxRecords;
    
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
     * private method to read main configuration file
     */
    private void readConfiguration() {
        updateLog("Start reading the configuration");
        
        // read some main configuration
        importFolder = ConfigPlugins.getPluginConfig(title).getString("importFolder");
        workflow = ConfigPlugins.getPluginConfig(title).getString("workflow");
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
            	// read the xml data files first
            	readTableAnmelder(importFolder + "Anmelder.xml");
                readTableMaster(importFolder + "Master.xml", importFolder + "Scans");
                readTablePrio(importFolder + "Prio.xml");

                updateLog("Start running through all import files");
            	int start = 0;
                int end = importEntries.values().size();
                itemsTotal = end - start;
                itemCurrent = start;
                
                // run through import objects
                for (ImportEntry ie : importEntries.values()) {
                    Thread.sleep(100);
                    if (!run) {
                        break;
                    }

                    // create a process name (here as UUID) and make sure it does not exist yet
                    String regex = ConfigurationHelper.getInstance().getProcessTitleReplacementRegex();
                    String processname = ie.getKey().replaceAll(regex, "_").trim();   
                    if (ProcessManager.countProcessTitle(processname, null) > 0) {
                        int tempCounter = 1;
                        String tempName = processname + "_" + tempCounter;
                        while(ProcessManager.countProcessTitle(tempName, null) > 0) {
                            tempCounter++;
                            tempName = processname + "_" + tempCounter;
                        }
                        processname = tempName;
                    }
                    updateLog("Start importing: " + processname, 1);
                    
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

                        Metadata mdKey = new Metadata(prefs.getMetadataTypeByName(metadataKey));
                        mdKey.setValue(ie.getKey());
                        logical.addMetadata(mdKey);

                        Metadata mdFullname = new Metadata(prefs.getMetadataTypeByName(metadataFullname));
                        mdFullname.setValue(ie.getFullname());
                        logical.addMetadata(mdFullname);
                        
                        Metadata mdPlace = new Metadata(prefs.getMetadataTypeByName(metadataPlace));
                        mdPlace.setValue(ie.getPlace());
                        logical.addMetadata(mdPlace);
                        
                        Metadata mdCountry = new Metadata(prefs.getMetadataTypeByName(metadataCountry));
                        mdCountry.setValue(ie.getCountry());
                        logical.addMetadata(mdCountry);
                        
                        Metadata mdDate = new Metadata(prefs.getMetadataTypeByName(metadataDate));
                        mdDate.setValue(ie.getDate());
                        logical.addMetadata(mdDate);
                        
                        Metadata mdTitle = new Metadata(prefs.getMetadataTypeByName(metadataTitle));
                        mdTitle.setValue(ie.getTitle());
                        logical.addMetadata(mdTitle);
                        
                        Metadata mdShelfmark = new Metadata(prefs.getMetadataTypeByName(metadataShelfmark));
                        mdShelfmark.setValue(ie.getShelfmark());
                        logical.addMetadata(mdShelfmark);
                        
                        Metadata mdPdf = new Metadata(prefs.getMetadataTypeByName(metadataPdf));
                        mdPdf.setValue(ie.getPdf());
                        logical.addMetadata(mdPdf);
                        
                        Metadata mdNotes = new Metadata(prefs.getMetadataTypeByName(metadataNotes));
                        mdNotes.setValue(ie.getNotes());
                        logical.addMetadata(mdNotes);
                        
                        Metadata mdFileName = new Metadata(prefs.getMetadataTypeByName(metadataFileName));
                        mdFileName.setValue(ie.getFileName());
                        logical.addMetadata(mdFileName);
                        
                        Metadata mdFilePath = new Metadata(prefs.getMetadataTypeByName(metadataFilePath));
                        mdFilePath.setValue(ie.getFilePath());
                        logical.addMetadata(mdFilePath);
                        
                        
//                        // create the metadata fields by reading the config (and get content from the content files of course)
//                        for (ImportSet importSet : importSets) {
//                            // treat persons different than regular metadata
//                            if (importSet.isPerson()) {
//                                logQueue.add(new LogMessage("Add person '" + importSet.getTarget() + "' with value '" + importSet.getSource() + "'", 0));
//                                Person p = new Person(prefs.getMetadataTypeByName(importSet.getTarget()));
//                                String firstname = importSet.getSource().substring(0, importSet.getSource().indexOf(" "));
//                                String lastname = importSet.getSource().substring(importSet.getSource().indexOf(" "));
//                                p.setFirstname(firstname);
//                                p.setLastname(lastname);
//                                logical.addPerson(p);       
//                            } else {
//                                logQueue.add(new LogMessage("Add metadata '" + importSet.getTarget() + "' with value '" + importSet.getSource() + "'", 0));
//                                Metadata mdTitle = new Metadata(prefs.getMetadataTypeByName(importSet.getTarget()));
//                                mdTitle.setValue(importSet.getSource());
//                                logical.addMetadata(mdTitle);
//                            }
//                        }

                        // save the process
                        Process process = bhelp.createAndSaveNewProcess(template, processname, fileformat);

                        // add some properties
                        bhelp.EigenschaftHinzufuegen(process, "Template", template.getTitel());
                        bhelp.EigenschaftHinzufuegen(process, "TemplateID", "" + template.getId());
                        ProcessManager.saveProcess(process);
                        
                        // if media files are given, import these into the media folder of the process
                        if (StringUtils.isNotBlank(ie.getFilePath())) {
                        	updateLog("Start copying media files, as they are available");
	                        File pdf = new File(ie.getFilePath());
	                        String targetBase = process.getImagesOrigDirectory(false);
	                        if (pdf.canRead()) {
	                            StorageProvider.getInstance().createDirectories(Paths.get(targetBase));
	                            StorageProvider.getInstance().copyFile(Paths.get(pdf.getAbsolutePath()), Paths.get(targetBase, ie.getFileName()));
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
            } catch (InterruptedException e) {
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
		updateLog("Import Anmelder: " + list.size());
		int counter = 0;
		
		for (Element e : list) {
			if (counter++ > maxRecords) {
				break;
			}
			updateLog("Importing anmelder with key: " + e.getChildText("Schluessel"));
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
		updateLog("Import Master: " + list.size());
		int counter = 0;
		for (Element e : list) {
			if (counter++ > maxRecords) {
				break;
			}
			
			// first get the key
			String myKey = e.getChildText("Schluessel");
			updateLog("Importing master with key: " + myKey);
			
			// now get the previously created element from the hashmap
			if (importEntries.containsKey(myKey)) {
				Collection<ImportEntry> col = importEntries.get(myKey);
				for (ImportEntry ie : col) {
					ie.setDate(e.getChildText("ErtDat"));
					ie.setTitle(e.getChildText("TitelNeu"));
					ie.setShelfmark(e.getChildText("AZNeu"));
					ie.setPdf(e.getChildText("PDFDoc"));
					ie.setNotes(e.getChildText("Bemerkung"));
					
					if (StringUtils.isNotBlank(ie.getShelfmark())){
						ie.setFileName(ie.getShelfmark().replace("/", "") + ".pdf");
						// try to find the pdf file in the file system
						String[] extensions = {"pdf"};
			            Collection<File> files = FileUtils.listFiles(new File(images), extensions, true);
			            for (File file : files) {
			                if (file.getName().equals(ie.getFileName())) {
			                	ie.setFilePath(file.getAbsolutePath());
			                }
			            }
				       
					}
				}
			} else {
				ImportEntry ie = new ImportEntry();
				ie.setKey(myKey);
				ie.setDate(e.getChildText("ErtDat"));
				ie.setTitle(e.getChildText("TitelNeu"));
				ie.setShelfmark(e.getChildText("AZNeu"));
				ie.setPdf(e.getChildText("PDFDoc"));
				ie.setNotes(e.getChildText("Bemerkung"));
				importEntries.put(ie.getKey(), ie);
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
		updateLog("Import Prio: " + list.size());
		int counter = 0;
		for (Element e : list) {
			if (counter++ > maxRecords) {
				break;
			}
			// first get the key and create a priority
			String myKey = e.getChildText("Schluessel");
			updateLog("Importing prio with key: " + myKey);
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
	 * @param logmessage
	 */
	private void updateLog(String logmessage) {
		updateLog(logmessage, 0);
	}
	
	/**
	 * simple method to send status message with specific level to gui
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
    
    public static void main(String[] args) {
		OepmaImporterWorkflowPlugin xd = new OepmaImporterWorkflowPlugin();
		xd.readTableAnmelder("/opt/digiverso/import/oepma/Anmelder.xml");
		xd.readTableMaster("/opt/digiverso/import/oepma/Master.xml", "/opt/digiverso/import/oepma/Scans");
		xd.readTablePrio("/opt/digiverso/import/oepma/Prio.xml");
		xd.printEntries();
	}
}
