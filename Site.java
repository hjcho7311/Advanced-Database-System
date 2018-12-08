import java.util.*;

/**
 * This class represents the site where data information is being stored. It contains site id, lock table, variable
 * list, and the las failed time.
 *
 * @author Hyung Jin Cho
 * @version 12/07/2018
 * @see SiteManager
 */
class Site {
  /**
   * Enum Site status: Site UP and Site DOWN
   */
  enum Status {
    UP, DOWN
  }

  private Status status;
  private final int siteId;
  private final Map<String, List<Lock>> lockTable;
  private final Map<String, Variable> variableMap;
  private int lastFailedTime;

  /**
   * Site constructor with given site id. Sets site status as UP and creates lock tables map and variableMap map.
   *
   * @param siteId The id of Site object
   * @return This site
   */
  Site(int siteId) {
    this.status = Status.UP;
    this.siteId = siteId;
    this.lockTable = new HashMap<>();
    this.variableMap = new HashMap<>();
    this.lastFailedTime = 0;
  }

  /**
   * Makes the site fail at given time, changes status as DOWN, stores failed time, and clears lock table.
   *
   * @param time The time of site failed
   */
  void fail(int time) {
    status = Status.DOWN;
    lastFailedTime = time;
    lockTable.clear();
  }

  /**
   * Recovers the site from failure and change status as UP.
   */
  void recover() {
    this.status = Status.UP;
    String variableId;
    int id;
    for (Map.Entry<String, Variable> variable : variableMap.entrySet()) {
      variableId = variable.getKey().substring(1);
      id = Integer.valueOf(variableId);
      if (id % 2 == 0) {
        variable.getValue().setIsReadable(false);
      }
    }
  }

  /**
   * Adds a variable into the variable list.
   *
   * @param variable The variable id
   * @throws NullPointerException if variable is null
   */
  void addVariableToSite(Variable variable) {
    if (!variableMap.containsValue(Objects.requireNonNull(variable, "variable must not be " + "null."))) {
      variableMap.put(variable.getVariableId(), variable);
    }
  }

  /**
   * Gets a list of variable id from the site.
   *
   * @return The variable list
   */
  List<String> getVariableIdList() {
    return new ArrayList<>(variableMap.keySet());
  }

  /**
   * Gets the variable from variable list with given variable id.
   *
   * @param variableId The id of variable
   * @return The variable object at given id
   * @throws NullPointerException if variableId is null
   */
  Variable getVariable(String variableId) {
    return Objects.requireNonNull(variableMap.get(variableId));
  }

  /**
   * Checks a lock table and conflict graph if variable can be read.
   *
   * @param transactionId The transaction id
   * @param variableId    The variable id
   * @param conflictGraph The wait for graph
   * @return true if the variable can read
   */
  boolean readLockVariable(String transactionId, String variableId, Map<String,
          Set<String>> conflictGraph) {
    if (!lockTable.containsKey(variableId)) {
      lockTable.put(variableId, new ArrayList<>());
    }
    List<Lock> locks = lockTable.get(variableId);
    if (locks.isEmpty()) {
      locks.add(new Lock(Lock.Type.READ, transactionId, variableId));
      return true;
    } else {
      Set<String> waiting = new HashSet<>();
      boolean isReadable = true;
      for (Lock lock : locks) {
        if (!isReadable || !lock.getTransactionId().equals(transactionId)) {
          if (lock.getType() == Lock.Type.WRITE) {
            waiting.add(lock.getTransactionId());
            isReadable = false;
          }
        } else {
          return true;
        }
      }
      if (!conflictGraph.containsKey(transactionId)) {
        conflictGraph.put(transactionId, new HashSet<>());
      }
      conflictGraph.get(transactionId).addAll(waiting);
      locks.add(new Lock(Lock.Type.READ, transactionId, variableId));

      return isReadable;
    }
  }

  /**
   * Reads the committed value from variable at given variable id for read-only.
   *
   * @param variableId The id of variable object
   * @param time       The time if requesting committed value
   * @return The committed value of variable at given time
   * @throws NullPointerException if variableId is null
   * @throws NullPointerException if time is null
   */
  int readVariable(String variableId, int time) {
    Variable variable = getVariable(Objects.requireNonNull(variableId, "variable id " + "must not be null."));
    return variable.readOnly(time);
  }

  /**
   * Reads the value of the variable with given variable id. If it is already committed, then read last committed value.
   *
   * @param variableId The id of variable object
   * @return The value of variable object
   * @throws NullPointerException if variableId is null
   * @throws NullPointerException if isCommitted is null
   */
  int readVariable(String variableId, boolean committed) {
    Variable variable = getVariable(Objects.requireNonNull(variableId, "variable id " + "must not be null."));
    if (committed) {
      return variable.getLastCommittedValue();
    }
    return variable.getValue();
  }

  /**
   * Writes the value of variable at site and changes the status of variable as it can be readable.
   *
   * @param variableId The id of variable object
   * @param value      The value of written value
   * @throws NullPointerException if variableId is null
   */
  void writeValueAtSite(String variableId, int value) {
    Variable variable = getVariable(Objects.requireNonNull(variableId, "variable id " + "must not be null."));
    variable.writeValue(value);
    variable.setIsReadable(true);
    variableMap.put(variableId, variable);
  }


  /**
   * Releases the locks if dead lock is detected from an aborted transaction.
   *
   * @param transaction The aborted transaction
   * @throws NullPointerException if transaction is null
   */
  void releaseLocksFromTable(Transaction transaction) {
    for (String lock : lockTable.keySet()) {
      List<Lock> get = lockTable.get(lock);
      for (int i = 0, getSize = get.size(); i < getSize; i++) {
        Lock key = get.get(i);
        String id = key.getTransactionId();
        if ((transaction.getTransactionId()).equals(id)) {
          lockTable.get(lock).remove(key);
          break;
        }
      }
    }
  }

  /**
   * Gets the status of site.
   *
   * @return The status of Site
   */

  Status getStatus() {
    return status;
  }

  /**
   * Gets the id of site.
   *
   * @return The id of site
   */
  int getSiteId() {
    return siteId;
  }

  /**
   * Gets the lock table of site.
   *
   * @return The lock table of site
   */
  Map<String, List<Lock>> getLockTable() {
    return lockTable;
  }

  /**
   * Gets the last failed time.
   *
   * @return The failed time
   */
  int getLastFailedTime() {
    return lastFailedTime;
  }
}
