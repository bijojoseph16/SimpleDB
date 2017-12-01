package simpledb.log;


import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import static simpledb.record.RecordPage.EMPTY;
import simpledb.file.Page;
import simpledb.buffer.PageFormatter;

public class LogFormatter implements PageFormatter{
	
	   
	   /**
	    * Creates a formatter for a new page of a table.
	    */
	   public LogFormatter() {
	      
	   }
	   
	   /** 
	    * Formats the page by allocating as many record slots
	    * as possible, given the record length.
	    * Each record slot is assigned a flag of EMPTY.
	    * Each integer field is given a value of 0, and
	    * each string field is given a value of "".
	    * @see simpledb.buffer.PageFormatter#format(simpledb.file.Page
	    * @author neetishpathak 
	    */
	   public void format(Page page) {
		   //do nothing
	   }

}
