export class TransformError extends Error {}

export class TransformBugError extends TransformError {
  constructor(public override message: string) {
    super(`[BUG] "${message}"`)
  }
}

export class NonExistColumnTransformError extends TransformError {
  constructor(public columnName: string) {
    super(`Accessing a non-existing column - "${columnName}"`)
  }
}

export class TransformStepError extends TransformError {
  constructor(
    public override message: string,
    public stepName: string
  ) {
    super(`[${stepName}] ${message}`)
  }
}

export class TransformAssertError extends TransformStepError {}
