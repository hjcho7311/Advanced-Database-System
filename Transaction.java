import java.util.*;

/**
 * This class represents a transaction of process that will be used by TransactionManager class. It contains all
 * information of a transaction.
 *
 * @author Hyung Jin Cho
 * @version 12/07/2018
 * @see TransactionManager
 */
class Transaction {
  /**
   * The enum type of transaction: RUNNING, WAITING, ABORTED, and COMMITTED
   */
  enum Status {
    RUNNING, WAITING, ABORTED, COMMITTED
  }

  final Map<Integer, Integer> accessSiteTime = new HashMap<>();
  private final String transactionId;
  private final int time;
  private final boolean isReadOnly;
  final List<String> checkedVariableIds = new ArrayList<>();
  private Operation operation;
  private boolean canCommit;
  Status status;

  /**
   * Constructor for transaction object.
   *
   * @param transactionId The id of transaction
   * @param time          The time of transaction executed
   * @param isReadOnly    The boolean variable to check if read only transaction
   * @throws NullPointerException if transactionId is null
   */
  Transaction(String transactionId, int time, boolean isReadOnly) {
    this.transactionId = Objects.requireNonNull(transactionId);
    this.time = time;
    this.isReadOnly = isReadOnly;
    this.canCommit = true;
  }

  /**
   * Adds operation into transaction object.
   *
   * @param operation The operation object
   * @throws NullPointerException if variableId is null
   */
  void addOperationToTransaction(Operation operation) {
    this.operation = Objects.requireNonNull(operation);
  }

  /**
   * Adds siteId if read or write operation was conducted.
   *
   * @param siteId The id of site
   * @param time   The time of transaction
   */
  void accessedSite(int siteId, int time) {
    if (!accessSiteTime.containsKey(siteId)) {
      accessSiteTime.put(siteId, time);
    }
  }

  /**
   * Gets the id of transaction.
   *
   * @return The id of transaction
   */
  String getTransactionId() {
    return transactionId;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  /**
   * Gets the time of transaction.
   *
   * @return The time of transaction
   */
  int getTime() {
    return time;
  }

  /**
   * Checks if transaction is read only.
   *
   * @return true if transaction is read only.
   */
  boolean getIsReadOnly() {
    return isReadOnly;
  }

  /**
   * Gets the operation object in transaction.
   *
   * @return The operation of transaction
   */
  Operation getOperation() {
    return operation;
  }

  /**
   * Checks if transaction can commit.
   *
   * @return true if transaction can commit, otherwise false
   */
  boolean getCanCommit() {
    return canCommit;
  }

  /**
   * Sets the canCommit
   *
   * @param canCommit The boolean variable of canCommit
   */
  void setCanCommit(boolean canCommit) {
    this.canCommit = canCommit;
  }
}
