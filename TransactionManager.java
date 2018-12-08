import java.util.*;

/**
 * This class translates read and write requests on variables to read and write requests on copies using the
 * available copy algorithm. It takes transaction id and time from Driver class and process transaction with deadlock
 * detection.
 *
 * @author Hyung Jin Cho
 * @version 12/07/2018
 * @see Transaction
 * @see Driver
 */
class TransactionManager {
  private static final int NUM_SITE = 10;
  private final Map<String, Transaction> transactions;
  private final Map<String, Set<String>> conflictGraph;
  private final List<String> waitingList;
  private final List<String> abortList;
  private final Driver driver;
  public final SiteManager siteManager;
  private final StringBuilder sb;

  /**
   * Constructor for transaction manager
   *
   * @param driver The driver object from Driver class
   * @throws NullPointerException if driver is null.
   */
  TransactionManager(Driver driver) {
    siteManager = new SiteManager();
    this.driver = Objects.requireNonNull(driver);
    transactions = new HashMap<>();
    conflictGraph = new HashMap<>();
    waitingList = new ArrayList<>();
    abortList = new ArrayList<>();
    sb = new StringBuilder();
  }

  /**
   * Begins a transaction by checking if it is read only transaction and put it in the transaction list.
   *
   * @param transactionId The id of the transaction
   * @param time          The time of the transaction
   * @param readOnly      The boolean variable if the transaction is read only
   * @throws NullPointerException if transactionId is null.
   */
  void beginTransaction(String transactionId, int time, boolean readOnly) {
    Transaction transaction = new Transaction(Objects.requireNonNull(transactionId,
            "transaction id must not be null"), time, readOnly);
    /*
    if (transactions.containsKey(transactionId)) {
      System.out.println("Error message: " + transactionId + " has already begun. Check your input again.");
      return;
    }
    */

    transactions.put(transactionId, transaction);
    if (transaction.getIsReadOnly()) {
      System.out.println(transactionId + " begins and is read-only");
    } else {
      System.out.println(transactionId + " begins");
    }
  }

  /**
   * Terminates a transaction with given id. It commits read and write request if it can commit.
   *
   * @param transactionId The transaction id of transaction object
   * @param time          The time of transaction
   * @throws NullPointerException if transactionId is null.
   */
  void endTransaction(String transactionId, int time) {
    Transaction transaction = transactions.get(Objects.requireNonNull(transactionId,
            "transaction id must not be null"));
    if (!transactions.containsKey(transactionId)) {
      System.out.println("Error message: " + transactionId + " has not begun yet. So it cannot end.");
      return;
    }
    if (waitingList.contains(transactionId) && isDeadLock("CheckPoint", transactionId)) {
      detectDeadlocks(transaction);
    } else if (abortList.contains(transactionId)) {
      System.out.println(transactionId + " was aborted");
      return;
    }
    // read case
    if (!transaction.getIsReadOnly()) {
      // check if variable was already written
      for (int siteId : transaction.accessSiteTime.keySet()) {
        Site site = siteManager.getSites().get(siteId - 1);
        Variable variable;
        for (String dirtyVariable : transaction.checkedVariableIds) {
          variable = site.getVariable(dirtyVariable);
          if (variable != null) {
            if (transaction.getCanCommit()) {
              variable.commitValue(time);
            } else {
              variable.recoverValue();
            }
          }
        }
      }

      Site site;
      int firstTouch;
      for (int siteId : transaction.accessSiteTime.keySet()) {
        site = siteManager.getSites().get(siteId - 1);
        // check if site was never touched
        firstTouch = transaction.accessSiteTime.get(siteId);
        if (site.getLastFailedTime() >= firstTouch) {
          transaction.setCanCommit(false);
          break;
        }
      }

      if (transaction.getCanCommit()) {
        System.out.println(transactionId + " commits");
      }
      if (!transaction.getCanCommit()) {
        System.out.println(transactionId + " aborts");
      }
      abortTransaction(transactionId, !transaction.getCanCommit());

    } else { // read only case
      for (int siteId : transaction.accessSiteTime.keySet()) {
        Site site = siteManager.getSites().get(siteId - 1);
        // if site is down cannot commit
        if (site.getStatus() == Site.Status.DOWN) {
          transaction.setCanCommit(false);
          break;
        }
      }

      if (!transaction.getCanCommit()) {
        Operation op = new Operation.Builder(Operation.Type.COMMIT).build();
        transaction.addOperationToTransaction(op);
        waitingList.add(transactionId);
        transaction.setStatus(Transaction.Status.WAITING);
//        System.out.println(transactionId + " aborts");
      } else {
//        System.out.println(transactionId + "reads value.");
        System.out.println(transactionId + " commits");
      }
    }
  }

  /**
   * Executes read request by checking its read lock. If it cannot get site by variable id, then it creates new
   * operation and add ito the waiting list. If it is read only transaction, it follows multi version read
   * consistency rule. If the transaction is on waiting list or abort list, it does not get read value.
   *
   * @param transactionId The transaction id
   * @param variableId    The variable id
   * @throws NullPointerException if transactionId is null.
   * @throws NullPointerException if variableId is null.
   */
  public void readRequest(String transactionId, String variableId) {
    Transaction transaction = transactions.get(Objects.requireNonNull(transactionId,
            "transaction id must not be null"));
    /*
    if (!transactions.containsKey(transactionId)) {
      System.out.println("Error message: " + transactionId + " has not begun yet. So it cannot read.");
      return;
    }
    */

    Site site = siteManager.getSite(variableId);

    // check waitingList and abortList
    if (waitingList.contains(transactionId)) {
      System.out.println("It cannot read since " + transactionId + " is still waiting.");
      return;
    } else if (abortList.contains(transactionId)) {
      System.out.println(transactionId +
              " was aborted so it failed to read from variable " + variableId + ".");
      return;
    }

    // if site is down, creates new operation object and set status as waiting until site is up again
    if (site == null) {
      Operation operation = new Operation.Builder(Operation.Type.READ).variableId
              (variableId).build();
      transaction.addOperationToTransaction(operation);
      waitingList.add(transactionId);
      transaction.setStatus(Transaction.Status.WAITING);
      System.out.println(transactionId + " cannot be performed since variable " +
              variableId + " was trying to access on failed site.");
      return;
    }

    // check read only case
    if (transaction.getIsReadOnly()) {
      int value = site.readVariable(variableId, transaction.getTime());
      transaction.accessedSite(site.getSiteId(), driver.time);
      System.out.println(transactionId + " reads value " + value + " from variable " + variableId);
      return;
    }

    // check deadlock case
    if (!site.readLockVariable(transactionId, variableId, conflictGraph)) { // cannot write case
      Operation operation = new Operation.Builder(Operation.Type.READ).variableId(variableId).build();
      transaction.addOperationToTransaction(operation);
      if (!isDeadLock("CheckPoint", transactionId)) {
        waitingList.add(transactionId);
        transaction.setStatus(Transaction.Status.WAITING);
        System.out.println(transactionId + " is waiting for " + variableId + ".");
      } else {
        detectDeadlocks(transactions.get(transactionId));
        if (!abortList.contains(transactionId)) {
          readRequest(transactionId, variableId);
        }
      }
    } else { // can write case
      int value = site.readVariable(variableId, false);
      System.out.println(transactionId + " got read lock to read value " + value + " " +
              "from variable " + variableId);
    }
    transaction.accessedSite(site.getSiteId(), driver.time);
  }


  /**
   * Executes write request by checking dead lock. If the transaction is on waiting list on abort list, or does  not
   * exist yet, it cannot write value.
   *
   * @param transactionId The transaction id
   * @param variableId    The variable id
   * @param value         The value of the variable
   * @throws NullPointerException if transactionId is null
   * @throws NullPointerException if variableId is null
   */
  public void writeRequest(String transactionId, String variableId, int value) {
    Transaction transaction = transactions.get(Objects.requireNonNull(transactionId,
            "transaction id must not be null"));
    if (!transactions.containsKey(transactionId)) {
      System.out.println("Error message: " + transactionId + " has not begun yet. So it cannot write.");
      return;
    }
    int id = Integer.valueOf(Objects.requireNonNull(variableId).substring(1));
    Site site;
    if (waitingList.contains(transactionId)) {
      System.out.println("It cannot write value on " + variableId +
              " since " + transactionId + " is still waiting.");
      return;
    } else if (abortList.contains(Objects.requireNonNull(transactionId))) {
      System.out.println("Failed to read " + variableId + " because " +
              transactionId + " was already aborted.");
      return;
    }

    // check if deadlock is detected
    if (!checkWrite(transactionId, variableId)) { // deadlock
      Operation operation = new Operation.Builder(Operation.Type.WRITE).variableId
              (variableId).value(value).build();
      transaction.addOperationToTransaction(operation);
      if (!isDeadLock("CheckPoint", transactionId)) {
        waitingList.add(transactionId);
        transaction.setStatus(Transaction.Status.WAITING);
        System.out.println(transactionId + " is waiting.");
      } else {
        detectDeadlocks(transactions.get(transactionId));
        if (!abortList.contains(transactionId)) {
          writeRequest(transactionId, variableId, value);
        }
      }
    } else {
      if (id % 2 == 0) { //  even indexed variable
        for (Site s : siteManager.getSites()) {
          if (s.getStatus() == Site.Status.UP) {
            s.writeValueAtSite(variableId, value);
          }

        }
        transactions.get(transactionId).checkedVariableIds.add(variableId);
        System.out.println(transactionId + " got write lock to write value " + value +
                " " + "on variable " + variableId + " at all available sites.");
      } else { // for even indexed variable, write on all sites
        int siteId = 1 + id % NUM_SITE;
        site = siteManager.getSites().get(siteId - 1);
        System.out.println(transactionId + " got write lock to write value " + value +
                " " + "on variable " + variableId + " at site " + site.getSiteId() + ".");
        if (site.getStatus() == Site.Status.UP) {
          site.writeValueAtSite(variableId, value);
        }
        transactions.get(transactionId).checkedVariableIds.add(variableId);
      }
    }
  }

  /**
   * Check if it can write.
   *
   * @param transactionId The id of transaction object
   * @param variableId    The id of variable object
   * @return The boolean if write operation can perform
   * @throws NullPointerException if transactionId is null
   * @throws NullPointerException if variableId is null
   */
  private boolean checkWrite(String transactionId, String variableId) {
    List<Site> sites = new ArrayList<>();
    int id = Integer.valueOf((variableId).substring(1));
    int count = 0;
    boolean result = true;

    if (id % 2 == 0) { // for odd indexed variable
      sites.addAll(siteManager.getSites());
    } else { // for even indexed variable
      int siteId = id + 1 % NUM_SITE;
      Site site = siteManager.getSites().get(siteId - 1);
      sites.add(site);
    }

    for (Site site : sites) {
      if (site.getStatus() == Site.Status.DOWN) {
        count++;
        System.out.println("It cannot write value on variable " + variableId + " at " +
                "failed site " + site.getSiteId() + ".");
        continue;
      }

      // check if lock is available
      transactions.get(transactionId).accessedSite(site.getSiteId(), driver.time);
      if (!site.getLockTable().containsKey(variableId) || site.getLockTable().get(variableId).isEmpty()) {
        List<Lock> locks = new ArrayList<>();
        locks.add(new Lock(Lock.Type.WRITE, Objects.requireNonNull(transactionId), variableId));
        site.getLockTable().put(variableId, locks);
      } else { // lock is available

        List<Lock> locks = site.getLockTable().get(variableId);
        if (transactionId.equals(locks.get(0).getTransactionId())) {
          if (Lock.Type.READ.equals(locks.get(0).getType())) {
            site.getLockTable().get(variableId).remove(0);
            site.getLockTable().get(variableId).add(0, new Lock(Lock.Type.WRITE,
                    transactionId, variableId));
          }
        } else {

          Set<String> waited = new HashSet<>();
          for (Lock lock : locks) {
            if (!transactionId.equals(lock.getTransactionId())) {
              waited.add(lock.getTransactionId());
            }
            if (!conflictGraph.containsKey(transactionId)) {
              conflictGraph.put(transactionId, new HashSet<>());
            }
            conflictGraph.get(transactionId).addAll(waited);
          }
          site.getLockTable().get(variableId).add(new Lock(Lock.Type.WRITE,
                  transactionId, variableId));
          result = false;
        }
      }
    }
    if (count == NUM_SITE) {
      result = false; //all the sites fail or no working sites has var
    }
    return result;
  }

  /**
   * Detects deadlocks by constructing a blocking graph and using depth-first-search(DFS) traverse.
   *
   * @param deadLockTransaction The id of transaction
   * @throws NullPointerException if transaction is null.
   */
  private void detectDeadlocks(Transaction deadLockTransaction) {
    List<String> tracking = new ArrayList<>();
    String transactionId = Objects.requireNonNull(deadLockTransaction).getTransactionId();
    List<String> cycle = new ArrayList<>();
    dfsTraverse(transactionId, cycle, tracking);
    System.out.println(transactionId + " aborts since it is youngest in the cycle.");


    int time = 0;
    Transaction transaction;
    for (String id : cycle) {
      transaction = transactions.get(id);
      if (transaction.getTime() > time) {
        time = transaction.getTime();
      }
    }
    abortTransaction(transactionId, true);
  }

  /**
   * Check if deadlock is exist.
   *
   * @param checking      The id of checking
   * @param transactionId The id of transaction
   * @return The boolean check if dead lock is detected
   * @throws NullPointerException if checking is null
   * @throws NullPointerException if transactionId is null
   */
  private boolean isDeadLock(String checking, String transactionId) {
    if (Objects.requireNonNull(transactionId, "transaction id is must not be null.").
            equals(Objects.requireNonNull(checking, "checking " + "is must not be null."))) {
      return true;
    }

    if (checking.equals("CheckPoint")) checking = transactionId;
    if (!(conflictGraph.containsKey(checking) && !conflictGraph.get(checking).isEmpty())) return false;
    return conflictGraph.get(checking).stream().anyMatch(next -> isDeadLock(next, transactionId));
  }

  /**
   * Recursive algorithms for depth-first-search. It uses a list of cycle and conflict graph.
   *
   * @param currentTransactionId The starting transaction in DFS
   * @param cycle                The cycle in DFS
   * @param tracking             The tracking in DFS
   * @throws NullPointerException if currentTransactionId is null.
   * @throws NullPointerException if cycle is null.
   */
  private void dfsTraverse(String currentTransactionId, List<String> cycle, List<String> tracking) {
    if (conflictGraph.size() != 0 && conflictGraph.containsKey(Objects.requireNonNull(currentTransactionId))) {
      Set<String> transactionIdSet = conflictGraph.get(currentTransactionId);
      for (String transactionId : transactionIdSet) {
        if (Objects.requireNonNull(cycle).contains(transactionId)) {
          tracking = new ArrayList<>(cycle);
        } else {
          cycle.add(transactionId);
          dfsTraverse(transactionId, cycle, tracking);
          cycle.remove(transactionId);
        }
      }
    }
  }

  /**
   * Aborts a transaction with given transaction id. Removes from waiting list, abort list, conflict graph, and
   * release lock from lock table. If there is next transaction that is waiting in the waiting list, then it executes
   * it.
   *
   * @param abortedTransactionId The id of aborted transaction
   * @param canAbort             The boolean check if it can abort
   * @throws NullPointerException if transactionId is null
   */
  private void abortTransaction(String abortedTransactionId, boolean canAbort) {
    Transaction abortedTransaction = transactions.get(Objects.requireNonNull(abortedTransactionId,
            "abortedTransactionId must not be null" + "."));
    if (!transactions.containsKey(abortedTransactionId)) {
      System.out.println("Error message: " + abortedTransactionId + " has not begun yet. So it cannot abort.");
      return;
    }
    Site site;
    for (int siteId : abortedTransaction.accessSiteTime.keySet()) {
      site = siteManager.getSites().get(siteId - 1);
      site.releaseLocksFromTable(abortedTransaction);
    }

    abortedTransaction.setStatus(Transaction.Status.ABORTED);
    waitingList.remove(abortedTransactionId);

    for (String transactionId : conflictGraph.keySet())
      conflictGraph.get(transactionId).stream().filter(next -> abortedTransaction.getTransactionId().equals(next))
              .findFirst().ifPresent(next -> conflictGraph.get(transactionId).remove(next));
    conflictGraph.remove(abortedTransaction.getTransactionId());

    if (canAbort) {
      abortList.add(abortedTransactionId);
      abortedTransaction.setStatus(Transaction.Status.ABORTED);
      Set<Integer> siteIds = abortedTransaction.accessSiteTime.keySet();
      Variable variable;
      for (int siteId : siteIds) {
        site = siteManager.getSites().get(siteId - 1);
        for (String dirtyVariableId : abortedTransaction.checkedVariableIds) {
          if (site.getVariable(dirtyVariableId) != null) {
            variable = site.getVariable(dirtyVariableId);
            variable.recoverValue();
          }
        }
      }
    }
    checkNextTransactionOnWaitingList();
  }

  /**
   * Runs next transaction which is waiting on waiting list. It might execute read, write, and commit unless it is
   * still blocked by another transaction.
   */
  private void checkNextTransactionOnWaitingList() {
    if (waitingList.isEmpty()) {
      return;
    }
    List<String> temp = new ArrayList<>(waitingList);
    for (String nextTid : temp) {
      if (!conflictGraph.containsKey(nextTid) || conflictGraph.get(nextTid).isEmpty()) {
        waitingList.remove(nextTid);
        String nextVid = transactions.get(nextTid).getOperation().getVariableId();
        Transaction t = transactions.get(nextTid);
        Operation operation = transactions.get(nextTid).getOperation();
        if (operation.getType() == Operation.Type.READ) {
          readRequest(nextTid, nextVid);
          t.setStatus(Transaction.Status.RUNNING);
        } else if (transactions.get(nextTid).getOperation().getType() == Operation.Type
                .WRITE) {
          int nextVal = transactions.get(nextTid).getOperation().getValue();
          writeRequest(nextTid, nextVid, nextVal);
          t.setStatus(Transaction.Status.RUNNING);
        } else {
          endTransaction(nextTid, driver.time);
          t.setStatus(Transaction.Status.COMMITTED);
        }
      } else {
        System.out.println(nextTid + " is still waiting.");
      }
    }
  }


  /**
   * If a transaction is already started with given site id, it aborts the transaction. Otherwise, adds transaction
   * into the set of aborted transaction.
   *
   * @param siteId site id
   * @param time   when the site fails
   */
  void failSite(int siteId, int time) {
    Set<String> abortedIdSet = new HashSet<>();
    Site site = siteManager.getSites().get(siteId - 1);
    site.getLockTable().forEach((key, value) -> {
      for (int i = 0; i < value.size(); i++) {
        Lock lock = value.get(i);
        String transactionId = lock.getTransactionId();
        abortedIdSet.add(transactionId);
      }
    });
    System.out.println("site " + siteId + " was failed ");
    for (String transactionId : abortedIdSet) {
      System.out.println("@Comment: " + transactionId + " was aborted because site " + siteId + " was failed.");
      abortTransaction(transactionId, true);
    }
    site.fail(time);
  }


  /**
   * Gives the committed values of all copies of all variables at all sites, sorted per
   * site with all values per site in ascending order by variable
   * e.g. site 1 – x1: 5, x2: 6, x3: 2, ... x20: 3
   */
  void dump() {
    for (int siteId = 1; siteId <= NUM_SITE; siteId++) {
      dump(siteId);
    }

    // use this commented code if it need to display all the value by variable.
    /* Display the committed values of all copies of all variables
    for (int i = 1; i <= NUM_VARIABLE; i++) {
      String vid = "x" + i;
      dump(vid);
    }
    */
  }

  /**
   * Gives the committed values of all copies of all variables at site in one line.
   *
   * @param siteId The id of site
   */
  void dump(int siteId) {
    Site site = siteManager.getSites().get(siteId - 1);
    List<String> variableIdList = site.getVariableIdList();
    System.out.print("Site " + site.getSiteId() + " - ");
    variableIdList.sort(new Comparator<String>() {
      public int compare(String o1, String o2) {
        return intCompare(o1) - intCompare(o2);
      }

      int intCompare(String s) {
        String num = s.replaceAll("\\D", "");
        return num.isEmpty() ? 0 : Integer.parseInt(num);
      }
    });

    for (String vid : variableIdList) {
      int index = Integer.valueOf(vid.substring(1));
      if (index % 2 == 1) {
        if ((index + 1 % 10) == siteId) {
          int value = site.readVariable(vid, true);
          System.out.print(vid + ": " + value + ", ");
        }
      } else if (index % 2 == 0) {
        if (index < 20) {
          int value = site.readVariable(vid, true);
          System.out.print(vid + ": " + value + ", ");
        } else {
          int value = site.readVariable(vid, true);
          System.out.print(vid + ": " + value);
        }
      }
    }
    System.out.println();
  }

  /**
   * Gives the committed values of all copies of variable xj at all sites, one line per
   * site.
   *
   * @param variableId the variable id
   */
  void dump(String variableId) {
    StringBuffer sb;
    int index = Integer.valueOf(variableId.substring(1));
    if (index % 2 == 0) {
      Map<Integer, List<Integer>> values = new HashMap<>();
      for (Site site : siteManager.getSites()) {
        int value = site.readVariable(variableId, true);
        if (!values.containsKey(value)) {
          values.put(value, new ArrayList<>());
        }
        List<Integer> valueAtSite = values.get(value);
        valueAtSite.add(site.getSiteId());
      }
      for (int v : values.keySet()) {
        if (values.size() != 1) {
          sb = new StringBuffer(variableId + ": ").append(v).append(" at site ");
          for (int s : values.get(v)) {
            sb.append(s).append(" ");
          }
          System.out.println(sb.toString().trim());
        } else {
          System.out.println(variableId + ": " + v + " at all available sites");
        }
      }
    } else {
      Site site;
      int siteId;
      siteId = 1 + index % NUM_SITE;
      site = siteManager.getSites().get(siteId - 1);
      int value;
      value = site.readVariable(variableId, true);
      System.out.println(variableId + ": " + value + " at site " + site.getSiteId());
    }
  }

  /**
   * Recovers site with given site id. If there is next transaction that is waiting in the waiting list after site
   * recover, then executes next transaction.
   *
   * @param siteId The id of site
   */
  void recoverSite(int siteId) {
    Site site = siteManager.getSites().get(siteId - 1);
    if (site != null && site.getStatus() == Site.Status.DOWN) {
      site.recover();
    }
    System.out.println("site " + siteId + " was recovered from failure.");
    checkNextTransactionOnWaitingList();
  }

  /**
   * Gives the state of each DM and the TM as well as the data distribution and data values.
   */
  public void querystate() {
    for (String transactionId : transactions.keySet()) {
      System.out.println(transactions.get(transactionId).toString());
    }
    System.out.println("Site Manger lock table: ");
    for (Site site : siteManager.getSites()) {
      System.out.println(site.getLockTable().toString());
    }
  }
}
