import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.tdb.*;
import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.*;

import javax.xml.ws.http.HTTPException;
import java.io.*;
import java.nio.file.Files;
import java.util.*;

/**
 * Yo yo honey singh
 * <p>
 * RDF
 *
 * @author: Arjun
 * @version: 0.0.1, 2015-07-17
 */
public class BulkNodeComparer {

    public static String[][] matrix;
    public static List<String> names = new ArrayList<String>();

    static Dataset dataset;
    static Model tdb;

    public static void main(String args[]) {
        // 0. setup
        setup();

        // 1. read data and get num to initialise matrix
        readdata();

        // 2. init matrix
        matrix = new String[names.size()+1][names.size()+1];

        // 3. populate tdb
        populatetdb(0);

        // 4. populate matrix
        populate();

        // 5. save data
        savedata();

    }

    public static void setup() {
        File file = new File("./BulkNodeComparerDB/tdb.lock");
        try {
            boolean result = Files.deleteIfExists(file.toPath());
            System.out.println("deleted in setup");
        }
        catch (IOException e){
            System.out.println("error in setup");
        }
    }

    /* readdata
    * 1: calculates the number of names in the file, also names.size();
    * 2: collects the names in the variable "names" */
    public static void readdata() {
        String file = "./bulkdata.txt";

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            String line = null;

            while ((line = reader.readLine()) != null) {
                names.add(line);
            }
        } catch (FileNotFoundException ex) {

        } catch (IOException ex) {

        }
    }



    /* populate tdb
    * loads data from DBpedia to the tdb */
    public static void populatetdb(int n) {
        // create the tdb
        String dir = "./NodeComparerDB";
        dataset = TDBFactory.createDataset(dir);
        tdb = dataset.getDefaultModel();

        tdb.removeAll();

        // go through all the names and grab the data
        while (n < names.size()) {
            String subject = geturistring(names.get(n));

            // get for s' data
            String q = "PREFIX dbres: <http://dbpedia.org/resource/> " +
                    "SELECT ?b ?x WHERE {" +
                    " dbres:" + subject + " ?b ?x . " +
                    "} ";
            Query query = QueryFactory.create(q);
            ResultSet r = null;
            QueryExecution qex = QueryExecutionFactory.sparqlService("http://dbpedia.org/sparql", query);
            try {
                r = qex.execSelect();
            }
            catch (HTTPException e) {
                System.out.println("There is an error with dbpedia. Please try again when the pages you require" +
                        " are no longer under maintenance.");
            }


            // Add all the data to the tdb
            Resource sub = ResourceFactory.createResource("http://dbpedia.org/resource/" + subject);

            while (r.hasNext()) {
                QuerySolution t = r.nextSolution();

                Property p = ResourceFactory.createProperty(t.getResource("b").toString());
                RDFNode o = t.get("x");
                tdb.add(sub, p, o);
            }

            // increment n
            n = n + 1;
        }

        System.out.println("we are done populating the tdb!");
    }

    public static String geturistring(String s) {
        // loop through all the characters. At a " " change it to a "_"
        String news = "";
        for (int i = 0; i<s.length(); i++) {
            if (s.charAt(i) == ' ') {
                news = news + '_';
            } else {
                news = news + s.charAt(i);
            }
        }
        return news;
    }




    public static void populate() {
        // 1. populate subject
        int col = 1;
        for (String subject : names) {
            matrix[col][0] = subject;
            matrix[0][col] = subject;
            col = col + 1;
        }

        // 2. calculate and populate
        for (int i=0; i<names.size()-1; i++) {
            for (int j=i+1; j<names.size(); j++) {
                calcnpop(i, j);
            }
        }

        // 3. print matrix
        for (int i = 0; i < names.size()+1; i++) {
            for (int j = 0; j<names.size()+1; j++) {
                System.out.print(matrix[i][j] + ", ");
            }
            System.out.println(" ");
        }
    }

    public static void calcnpop(int curr, int next) {
        // query curr tdb - count all of them
        String q1 =  "PREFIX dbres: <http://dbpedia.org/resource/> " +
                "SELECT ?b ?x WHERE { " +
                " dbres:" + geturistring(names.get(curr)) + " ?b ?x . " +
                "} ";
        Query query1 = QueryFactory.create(q1);
        QueryExecution qex1 = QueryExecutionFactory.create(query1, tdb);
        ResultSet r1 = qex1.execSelect();

        // go through each one and get the property and object
        int score = 0;
        int currtriples = 0;
        int nexttriples = 0;
        while (r1.hasNext()) {
            QuerySolution t1 = r1.nextSolution();
            String p1 = t1.getResource("b").toString();
            String o1 = t1.get("x").toString();

            currtriples = currtriples + 1;


            // query next tdb - count all of them
            String q2 =  "PREFIX dbres: <http://dbpedia.org/resource/> " +
                    "SELECT ?b ?x WHERE { " +
                    " dbres:" + geturistring(names.get(next)) + " ?b ?x . " +
                    "} ";
            Query query2 = QueryFactory.create(q2);
            QueryExecution qex2 = QueryExecutionFactory.create(query2, tdb);
            ResultSet r2 = qex2.execSelect();

            // 2. go through each one and get the property and object
            nexttriples = 0;
            while (r2.hasNext()) {
                QuerySolution t2 = r2.nextSolution();
                String p2 = t2.getResource("b").toString();
                String o2 = t2.get("x").toString();

                nexttriples = nexttriples + 1;

                // comparing the data
                if (p1.equals(p2) && o1.equals(o2)) {
                    // if a similar triple is found
                    // System.out.format("|    %-50s |    %s \n", p2, o2);
                    score = score + 1;
                } else {
                    // if a similar triple is not found
                    score = score + 0;
                }


            }
        }

        // count similar
        //System.out.println("number of similar: " + score);
        //System.out.println("number of currtriples: " + currtriples);
        //System.out.println("number of nexttriples: " + nexttriples);

        // populate matrix with proper divisor
        matrix[curr+1][next+1] = Float.toString(((float)(score)/(float)(currtriples)) * 100);
        matrix[next+1][curr+1] = Float.toString(((float)(score)/(float)(nexttriples)) * 100);
    }




    public static void savedata() {
        // use the library
        HSSFWorkbook workbook = new HSSFWorkbook();
        HSSFSheet sheet = workbook.createSheet("BulkComparerSheet");

        //Need to transfer matrix into sheet!
        for (int i=0; i<matrix.length; i++) {
            //Create a new row in current sheet
            Row row = sheet.createRow(i);

            for (int j=0; j<matrix[0].length; j++) {
                Cell cell = row.createCell(j);
                if (matrix[i][j] == null) {
                    cell.setCellValue("-");
                } else {
                    cell.setCellValue(matrix[i][j]);
                }
            }
        }

        // save file
        try {
            FileOutputStream out = new FileOutputStream(new File("./BulkComparer.xls"));
            workbook.write(out);
            out.close();
            System.out.println("Excel written successfully..");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
