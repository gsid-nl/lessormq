package dev.lessormq;

/** One invalid field of a rejected request. */
public record FieldError(String field, String message) {
}
