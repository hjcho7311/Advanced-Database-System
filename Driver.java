import java.io.*;
import java.util.Objects;

/**
 * <h1>Replicated Concurrency Control and Recovery(RepCRec)</h1>
 * It contains a distributed database, complete with multiversion concurrency control, deadlock detection,
 * replication, and failure recovery.
 * <p>
 * This class represents the driver of RepCRec project. It reads input file, parses it and executes the instruction.
 *
 * @author Hyung Jin Cho
 * @version 12/07/2018
 * @see TransactionManager
 */
class Driver {
  private final TransactionManager tm;
  int time = 1;

  private Driver() {
    tm = new TransactionManager(this);
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("Please provide input file.");
      System.exit(1);
    }
    Driver driver = new Driver();
    driver.readFromFile(args[0]);
  }

  /**
   * Read each line from input file and parse instruction.
   *
   * @param inputFile, The name of the input file
   */
  private void readFromFile(String inputFile) {
    FileReader fileReader;
    BufferedReader br;
    try {
      fileReader = new FileReader(Objects.requireNonNull(inputFile, "inputFile must not be null."));
      br = new BufferedReader(fileReader);
      System.out.println(">>>> Input file name: " + inputFile);
      String line = br.readLine();

      while (line != null) {
        if (line.startsWith("//") || line.isEmpty()) {
          line = br.readLine();
          continue;
        } else if (line.startsWith("=")) {
          break;
        }

        String[] commands = line.split(";");
        for (String command : commands) {
          String temp = command.trim();
          String instruction = temp.substring(0, temp.indexOf("("))
                  .trim();
          String transactionId = temp.substring(
                  temp.indexOf("(") + 1, temp.indexOf(")")).trim();

          String tid;
          String vid;
          int siteId;
          int val;
          switch (instruction.toLowerCase()) {
            case "begin":
              tm.beginTransaction(transactionId, time, false);
              break;

            case "beginro":
              tm.beginTransaction(transactionId, time, true);
              break;

            case "end":
              tm.endTransaction(transactionId, time);
              break;

            case "fail":
              siteId = Integer.parseInt(transactionId);
              tm.failSite(siteId, time);
              break;

            case "recover":
              siteId = Integer.parseInt(transactionId);
              tm.recoverSite(siteId);
              break;

            case "w":
              tid = transactionId.split(",")[0].trim();
              vid = transactionId.split(",")[1].trim();
              val = Integer.parseInt(transactionId.split(",")[2].trim());
              tm.writeRequest(tid, vid, val);
              break;

            case "r":
              tid = transactionId.split(",")[0].trim();
              vid = transactionId.split(",")[1].trim();
              tm.readRequest(tid, vid);
              break;

            case "dump":
              if (transactionId.equals("")) {
                tm.dump();
              } else if (transactionId.toLowerCase()
                      .startsWith("x")) {
                tm.dump(transactionId);
              } else {
                int siteID = Integer.parseInt(transactionId);
                tm.dump(siteID);
              }
              break;

            default:
              System.out.println("Error: instruction might be invalid.");
          }
        }
        time++;
        line = br.readLine();
      }
      System.out.println();
      br.close();
    } catch (Exception e) {
      System.out.println("Error: " + e.getMessage());
    }
  }
}
