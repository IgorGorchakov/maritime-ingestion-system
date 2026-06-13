package com.maritime.common.validation;

/**
 * Immutable result of a vessel event validation check.
 */
public class ValidationResult {

    private final boolean valid;
    private final String reason; // null when valid

    private ValidationResult(boolean valid, String reason) {
        this.valid = valid;
        this.reason = reason;
    }

    public static ValidationResult valid() {
        return new ValidationResult(true, null);
    }

    public static ValidationResult invalid(String reason) {
        return new ValidationResult(false, reason);
    }

    public boolean isValid() {
        return valid;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return valid ? "VALID" : "INVALID: " + reason;
    }
}
