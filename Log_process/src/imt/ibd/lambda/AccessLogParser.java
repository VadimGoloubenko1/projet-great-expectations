package imt.ibd.lambda;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher; 
import java.util.regex.Pattern;


public class AccessLogParser {
	
    // Creating a regular expression for the records 
    final String regex = "^(\\S+) (\\S+) (\\S+) \\[([\\w:\\/]+\\s[+\\-]\\d{4})\\]\\s+\\\"(\\w*) (\\S+)\\s*(\\S+)?\\s*\\\" (\\d{3}) (\\S+)";  
    
    final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE); 
	
	
    public List<AccessLogEntry> parse(String filePath) throws IOException {
    	File f = new File(filePath);
    	BufferedReader b = new BufferedReader(new FileReader(f));

    	List<AccessLogEntry> entries = new LinkedList<AccessLogEntry>();
    	
    	String readLine = "";
    	while ((readLine = b.readLine()) != null) {
    		SimpleDateFormat formater = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss", Locale.US);
    		
    		Matcher matcher = pattern.matcher(readLine);
    		while (matcher.find()) { 
    			try {
    				String IP = matcher.group(1);
    				String dateStr = matcher.group(4);
    				String method = matcher.group(5);
    				String url = matcher.group(6);
    				String statusStr = matcher.group(8);
    				String sizeStr = matcher.group(9);

    				int status = Integer.parseInt(statusStr);
    				int size = Integer.parseInt(sizeStr);
            		
    				Date date;		    		
    				int index = dateStr.indexOf("-");
    				if (index == -1) {
    					index = dateStr.indexOf("+");
    				}
    				dateStr = dateStr.substring(0, index);
    				date = formater.parse(dateStr);
    				Calendar calendar = Calendar.getInstance();
    				calendar.setTime(date);
    				
    				AccessLogEntry entry = new AccessLogEntry(IP, calendar, method, url, status, size);
    				entries.add(entry);
    			} catch (Exception e) {
    				// e.printStackTrace();
    			}
            }
    	}
        return entries;    	
    } 
    
} 