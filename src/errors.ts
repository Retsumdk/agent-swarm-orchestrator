export type ErrorCode =
  | "VALIDATION"
  | "NOT_FOUND"
  | "CONFLICT"
  | "CORRUPTION"
  | "CONFIG";

export class SwarmError extends Error {
  readonly code: ErrorCode;
  readonly httpStatus: number;

  constructor(code: ErrorCode, message: string, httpStatus: number = 400) {
    super(message);
    this.name = "SwarmError";
    this.code = code;
    this.httpStatus = httpStatus;
  }
}

export function validationError(message: string): SwarmError {
  return new SwarmError("VALIDATION", message, 400);
}

export function notFoundError(message: string): SwarmError {
  return new SwarmError("NOT_FOUND", message, 404);
}

export function conflictError(message: string): SwarmError {
  return new SwarmError("CONFLICT", message, 409);
}

export function corruptionError(message: string): SwarmError {
  return new SwarmError("CORRUPTION", message, 500);
}

export function configError(message: string): SwarmError {
  return new SwarmError("CONFIG", message, 400);
}
