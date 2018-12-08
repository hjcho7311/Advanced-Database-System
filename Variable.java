import java.util.*;

/**
 * This class represents data items on each site.
 *
 * @author Hyung Jin Cho
 * @version 12/07/2018
 */
class Variable {
  private final String variableId;
  private int value;
  private final Map<Integer, Integer> previousValue = new HashMap<>();
  private int lastCommittedValue;
  private boolean isReadable;
  private boolean isWritable;


  /**
   * Variable constructor with given variable id.
   *
   * @param variableId The id of Variable object
   * @return This variable
   * @throws NullPointerException if variableId is null.
   */
  Variable(String variableId) {
    this.variableId = variableId;
    this.value = initializeValue(variableId);
    this.previousValue.put(0, value);
    this.lastCommittedValue = value;
    this.isReadable = true;
  }

  /**
   * Initializes the value of variable.
   *
   * @param variableId The id of variable object
   * @return The value of the variable object
   * @throws NullPointerException if variableId is null.
   */
  private int initializeValue(String variableId) {
    String temp = Objects.requireNonNull(variableId).replace("x", "");
    int id = Integer.valueOf(temp);
    return id * 10;
  }

  /**
   * Gets the value of variable.
   *
   * @return The value of variable
   */
  int getValue() {
    return this.value;
  }

  /**
   * Read the last committed value before the given timestamp, for Read-Only transaction.
   *
   * @param time the given transaction timestamp
   * @return last committed value before the timestamp
   */
  int readOnly(int time) {
    List<Integer> previousValueTimes = new ArrayList<>(previousValue.keySet());
    int size = previousValueTimes.size();
    Collections.sort(previousValueTimes);
    if (time > previousValueTimes.get(previousValueTimes.size() - 1)) {
      return previousValue.get(previousValueTimes.get(size - 1));
    } else {
      int low = 0;
      int high = size - 1;
      int mid;
      if (high - low <= 1) {
        return previousValue.get(previousValueTimes.get(low));
      }
      mid = low + ((high - low) / 2);
      int temp = previousValueTimes.get(mid);
      if (temp >= time) {
        high = mid;
      } else {
        low = mid;
      }
      while (high - low > 1) {
        mid = low + ((high - low) / 2);
        temp = previousValueTimes.get(mid);
        if (temp >= time) {
          high = mid;
        } else {
          low = mid;
        }
      }
      return previousValue.get(previousValueTimes.get(low));
    }
  }


  /**
   * Commits the value of variable for commit operation.
   *
   * @param time The time of variable when commit
   * @throws NullPointerException if time is null.
   */
  void commitValue(int time) {
    this.previousValue.put(time, value);
    this.lastCommittedValue = value;
  }


  /**
   * Writes the value of variable for write operation.
   *
   * @param value The value of variable
   * @throws NullPointerException if value is null.
   */
  void writeValue(int value) {
    this.value = value;
  }


  /**
   * When a transaction is aborted, it recovers value from the last committed value.
   */
  void recoverValue() {
    this.value = lastCommittedValue;
  }

  /**
   * Gets the last committed value of variable for abort operation.
   *
   * @return The last committed value of variable
   */
  int getLastCommittedValue() {
    return this.lastCommittedValue;
  }

  /**
   * Gets the variable id.
   *
   * @return The variable id
   */
  String getVariableId() {
    return variableId;
  }

  /**
   * Check if the variable can be read for Site recovery period
   *
   * @return true if the variable is allowed to read, false otherwise
   */
  boolean getIsReadable() {
    return isReadable;
  }

  /**
   * Sets boolean isReadable.
   *
   * @return true if variable can be read
   */
  void setIsReadable(boolean isReadable) {
    this.isReadable = isReadable;
  }


  /**
   * Check if the variable can be written.
   *
   * @return true if the variable can be written, false otherwise
   */
  public boolean getIsWritable() {
    return isWritable;
  }

  /**
   * Sets boolean isWritable
   *
   * @return true if variable can be written
   */
  public void setIsWritable(boolean isWritable) {
    isWritable = isWritable;
  }
}
