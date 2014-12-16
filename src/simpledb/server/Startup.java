package simpledb.server;

import simpledb.remote.*;
import java.rmi.registry.*;

public class Startup {
   public static void main(String args[]) throws Exception {
      // configure and initialize the database
      String database = args.length == 0 ? "StudentDB" : args[0];
      int gClockRefCount = args.length == 1 ? 5 : Integer.parseInt(args[1]);
      
      SimpleDB.init(database, gClockRefCount);
      
      // create a registry specific for the server on the default port
      Registry reg = LocateRegistry.createRegistry(1099);
      
      // and post the server entry in it
      RemoteDriver d = new RemoteDriverImpl();
      reg.rebind("simpledb", d);
      
      System.out.println("database server ready");
   }
}
