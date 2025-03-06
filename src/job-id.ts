import { v4 as uuid } from 'uuid';

/**
 * Utility class for managing job IDs in a consistent format.
 * All job ID operations should go through this class to maintain consistency.
 */
export class JobId {
  /**
   * Get the separator used in job IDs
   * @returns {string} The separator character
   */
  private readonly separator: string;

  /**
   * Get the prefix used in job IDs
   * @returns {string} The prefix string
   */
  private readonly prefix: string;

  /**
   * Get the regex pattern for validating job IDs
   * @returns {RegExp} The regex pattern
   */
  private readonly regexp: RegExp;

  constructor(separator?: string, prefix?: string) {
    this.separator = separator || ':';
    this.prefix = prefix || 'job';

    // /^job:([^:]+):([a-f0-9-]+)$/;
    this.regexp = new RegExp(
      `^${this.prefix}${this.separator}([^:]+)${this.separator}([a-f0-9-]+)$`,
    );
  }

  /**
   * Generates a unique job ID with a consistent format
   * @param {string} jobName - The name of the job
   * @returns {string} A formatted job ID
   */
  public generate(jobName: string): string {
    const uniqueId = uuid();

    return [this.prefix, jobName, uniqueId].join(this.separator);
  }

  /**
   * Extracts the job name from a job ID
   * @param {string} id - The job ID to parse
   * @returns {string} The job name
   * @throws {Error} If the ID format is invalid
   */
  public getJobName(id: string): string {
    const match = this.regexp.exec(id);

    if (!match) {
      throw new Error(`Invalid job ID format: ${id}`);
    }

    return match[1];
  }

  /**
   * Validates if a string is a valid job ID
   * @param {string} id - The string to validate
   * @returns {boolean} True if the ID is valid
   */
  public isValid(id: string): boolean {
    return this.regexp.test(id);
  }

  /**
   * Gets the UUID from a job ID
   * @param {string} id - The job ID to parse
   * @returns {string} The UUID
   * @throws {Error} If the ID format is invalid
   */
  public getUuid(id: string): string {
    const match = this.regexp.exec(id);

    if (!match) {
      throw new Error(`Invalid job ID format: ${id}`);
    }

    return match[2];
  }

  /**
   * Creates a job ID from components
   * @param {string} jobName - The name of the job
   * @param {string} [uniqueId] - Optional UUID (defaults to a new UUID)
   * @returns {string} A formatted job ID
   */
  public fromComponents(jobName: string, uniqueId?: string): string {
    return [this.prefix, jobName, uniqueId ?? uuid()].join(this.separator);
  }
}
