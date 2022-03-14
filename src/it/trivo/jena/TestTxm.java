/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.trivo.jena;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.modify.UpdateResult;
import org.apache.jena.system.Txn;
import org.apache.jena.update.UpdateAction;


/**
 *
 * @author Lorenzo
 */
public class TestTxm {
    private static Dataset dataset;
    
    private final static String __insertData = 
        "PREFIX mp: <http://mysparql.com/>" + System.lineSeparator() + 
        "INSERT DATA {" + System.lineSeparator() + 
        "mp:person0 mp:firstname \"Jay\" ." + System.lineSeparator() + 
        "mp:person0 mp:lastname \"Stevens\" ." + System.lineSeparator() + 
        "mp:person0 mp:state \"CA\" ." + System.lineSeparator() + 
        "mp:person1 mp:firstname \"John\" ." + System.lineSeparator() + 
        "mp:person1 mp:lastname \"Homlmes\" ." + System.lineSeparator() + 
        "mp:person1 mp:state \"CZ\" ." + System.lineSeparator() + 
        "mp:person2 mp:firstname \"Erwin\" ." + System.lineSeparator() + 
        "mp:person2 mp:lastname \"Rommel\" ." + System.lineSeparator() + 
        "mp:person2 mp:state \"PZ\" ." + System.lineSeparator() +
        "}"
    ;
    
    private final static String __deleteData = 
        "PREFIX mp: <http://mysparql.com/>" + System.lineSeparator()    +

        "DELETE DATA {" + System.lineSeparator()                        +
        "mp:person0 mp:firstname \"Jay\" ." + System.lineSeparator()    +
        "}";
    
         
    private final static String __update1 = 
        "PREFIX mp: <http://mysparql.com/>" + System.lineSeparator() +
        "DELETE  { ?s mp:state 'CZ' }" + System.lineSeparator() +
        "INSERT  { ?s mp:state 'UZ' }" + System.lineSeparator() +
        "WHERE {?s mp:state 'CZ' }"
    ;
    
    private final static String __complicate_1 = 
        "PREFIX schema:<http://schema.org/> " + System.lineSeparator() +
        "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> " + System.lineSeparator() +
        "PREFIX chat:<http://wot.arces.unibo.it/chat#> " + System.lineSeparator() +
        "WITH <http://wot.arces.unibo.it/chat/> DELETE {?x rdf:type schema:Person . ?x schema:name <http://o>} INSERT {?person rdf:type schema:Person ; schema:name <http://o>}   " + System.lineSeparator() +
        "WHERE {BIND(IRI(CONCAT(\"http://wot.arces.unibo.it/chat/person_\",STRUUID())) AS ?person) OPTIONAL {?x rdf:type schema:Person . ?x schema:name <http://o>}} " + System.lineSeparator() ;

    private static void doTests() {
            query("SELECT * WHERE {GRAPH ?g { ?s ?p ?o }}");

            update(__insertData);   //do a bulk insert
            
            
            update(__insertData);   //do a bulk insert without results
            
            update(__deleteData);   //do a bulk delete
            
            update(__deleteData);   //do a bulk delete without results

            query("SELECT * WHERE {?firstName ?lastName ?state }");
            
            update(__update1);        
    }
    
    private static void doMemTests() {
        dataset = DatasetFactory.createTxnMem();
        doTests();
    }
    
    private static void doTDB2Tests() {
        final org.apache.jena.dboe.base.file.Location loc = org.apache.jena.dboe.base.file.Location.create("./data2");
        dataset = org.apache.jena.tdb2.TDB2Factory.connectDataset(loc);       
        doTests();
    }
    private static void doTDB1Tests() {
        final org.apache.jena.tdb.base.file.Location loc = org.apache.jena.tdb.base.file.Location.create("./data");
        dataset = org.apache.jena.tdb.TDBFactory.createDataset(loc);
        doTests();
    }
    
    public static void main(String[] args) {
        try {

            
            final boolean fUseMem = System.getProperty("USE_MEM") != null;
            final boolean fUseTDB2 = System.getProperty("USE_TDB2") != null;
            //dataset = dss;
            if (fUseMem)
                doMemTests();
            else {
                if (fUseTDB2)
                    doTDB2Tests();
                else
                    doTDB1Tests();
            }
            
            //update(__complicate_1);   //do a bulk insert
            


        } catch(Throwable e)         {
            System.err.println(e);
            e.printStackTrace(System.err);
        }
        return;
    }
    
    public static void query(String query) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            RDFConnection conn = RDFConnectionFactory.connect(dataset);
            Txn.executeRead(conn, ()-> {
                    ResultSet rs = conn.query(QueryFactory.create(query)).execSelect();
                    ResultSetFormatter.outputAsJSON(out, rs);
            });

            try {
                    System.out.println("Query output : " + out.toString(StandardCharsets.UTF_8.name()));
            } catch (UnsupportedEncodingException e) {
                    System.err.println(e);
            }
    }


    public static void update(String query) {
            RDFConnection conn = RDFConnectionFactory.connect(dataset);
            
            Txn.executeWrite(conn, ()-> {
                    final List<UpdateResult> ur = conn.update(query);
                    if (ur != null) {
                        System.out.println("Update output ");
                        for(final UpdateResult u : ur ) {
                            System.out.println("******************");
                            System.out.println(u == null ? "<null>" : u.toString());
                        }
                    }
            });
            
    }
    public static void insertData(String query) {
            UpdateAction.parseExecute(query, dataset);
            RDFDataMgr.write(System.out, dataset, Lang.TRIG);
            
    }

}
