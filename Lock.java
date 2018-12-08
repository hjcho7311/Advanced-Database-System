/**
 * This class represents the lock. It has two type of lock: Read and Write. It also contains transaction id and
 * variable id.
 *
 * @author Hyung Jin Cho
 * @version 12/07/2018
 */
class Lock {
  /**
   * The enum type of lock: Read and Write
   */
  enum Type {
    READ, WRITE
  }

  private final Type type;
  private final String transactionId;
  private final String variableId;

  /**
   * Constructor for lock object. Creates a new lock object with given type, transaction id, variable id.
   *
   * @param type          The type of the lock
   * @param transactionId The id of transaction
   * @param variableId    The id of variable
   * @return This lock
   */
  Lock(Type type, String transactionId, String variableId) {
    this.type = type;
    this.transactionId = transactionId;
    this.variableId = variableId;
  }

  /**
   * Gets the type of lock
   *
   * @return The type of lock
   */
  Type getType() {
    return type;
  }

  /**
   * Gets the transaction id
   *
   * @return The id of transaction
   */
  String getTransactionId() {
    return transactionId;
  }
}
