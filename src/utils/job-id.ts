import { v4 as uuid } from 'uuid';

const PARTS_LEN = 2;

export const JobId = {
  /**
   * Generate a job ID with a consistent format
   * @param {string} jobName - The name of the job
   * @returns {string} The generated job ID
   */
  generate(jobName: string): string {
    return `${jobName}:${uuid()}`;
  },

  /**
   * Parse a job ID to extract its components
   * @param {string} id - The job ID to parse
   * @returns {{ jobName: string; uuid: string } | null} The parsed components or null if invalid
   */
  parse(id: string): { jobName: string; jobId: string } | null {
    const parts = id.split(':');

    if (parts.length !== PARTS_LEN) return null;

    const [jobName, jobId] = parts;

    if (!jobName || !jobId) return null;

    return { jobName, jobId };
  },

  /**
   * Extract the job name from a job ID
   * @param {string} id - The job ID to parse
   * @returns {string | null} The job name or null if invalid
   */
  getJobName(id: string): string | null {
    const parsed = JobId.parse(id);

    return parsed?.jobName ?? null;
  },

  /**
   * Validate if a string is a valid job ID
   * @param {string} id - The ID to validate
   * @returns {boolean} Whether the ID is valid
   */
  isValid(id: string): boolean {
    return JobId.parse(id) !== null;
  },
};
