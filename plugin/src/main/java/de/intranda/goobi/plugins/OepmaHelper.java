package de.intranda.goobi.plugins;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;

import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

public class OepmaHelper {

	/**
     * simple sax parser using JDom2 for the given xml file
     * 
     * @param fileName
     * @return the read jdom document
     */
	public static Document getSAXParsedDocument(final String fileName) {
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
     * File filter for xml files
     */
    public static final DirectoryStream.Filter<Path> xmlFilter = new DirectoryStream.Filter<Path>() {
        @Override
        public boolean accept(Path path) {
            return path.getFileName().toString().endsWith(".xml");
        }
    };
}
