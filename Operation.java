import java.util.Objects;

/**
 * This class represents READ, WRITE, COMMIT operations.
 *
 * @author Hyung Jin Cho
 * @version 12/07/2018
 */
class Operation {
  /**
   * Enum Operation types: Read, Write and Commit
   */
  enum Type {
    READ, WRITE, COMMIT
  }

  private final Type type;
  private final String variableId;
  private final int value;

  /**
   * Inner builder class of Operation
   */
  static final class Builder {
    private final Type type;
    private String variableId = "";
    private int value = 0;

    /**
     * Creates a Operation object with type. The type of Operation is required.
     *
     * @param type The non-null type of Operation
     * @return This builder
     * @throws NullPointerException if type is null
     */
    Builder(Type type) {
      this.type = Objects.requireNonNull(type, "type must not be null.");
    }

    /**
     * Sets the variableId of Operation object, optional.
     *
     * @param variableId non-null and any string including empty
     * @return The variableId
     * @throws NullPointerException if variableId is null
     */
    Builder variableId(String variableId) {
      this.variableId = variableId;
      return this;
    }

    /**
     * Sets the variableId of Operation object, optional.
     *
     * @param value The non-null value of Operation
     * @return The value.
     * @throws NullPointerException if value is null
     */
    Builder value(int value) {
      this.value = value;
      return this;
    }

    /**
     * Constructs an Build.
     *
     * @return a Operation object
     */
    Operation build() {
      return new Operation(this);
    }
  }

  /**
   * Create a instance of Operation class.
   *
   * @param builder to create a Operation object
   */
  private Operation(Builder builder) {
    type = builder.type;
    variableId = builder.variableId;
    value = builder.value;
  }

  /**
   * Gets the type of Operation
   */
  Type getType() {
    return type;
  }

  /**
   * Gets the variableId of Operation
   */
  String getVariableId() {
    return variableId;
  }

  /**
   * Gets the value of operation
   */
  int getValue() {
    return value;
  }
}
