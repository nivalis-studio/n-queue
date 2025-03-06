import { describe, expect, test } from 'bun:test';
import { v4 as uuid } from 'uuid';
import { JobId } from './job-id';

describe('JobId', () => {
  const jobId = new JobId(':', 'job');

  describe('generate', () => {
    test('should generate a valid job ID', () => {
      const id = jobId.generate('test-job');

      expect(id).toMatch(/^job:test-job:[a-f0-9-]+$/);
      expect(jobId.isValid(id)).toBe(true);
    });
  });

  describe('getJobName', () => {
    test('should extract job name from valid ID', () => {
      const id = jobId.generate('test-job');

      expect(jobId.getJobName(id)).toBe('test-job');
    });

    test('should throw error for invalid ID', () => {
      expect(() => jobId.getJobName('invalid-id')).toThrow(
        'Invalid job ID format',
      );
    });
  });

  describe('isValid', () => {
    test('should return true for valid ID', () => {
      const id = jobId.generate('test-job');

      expect(jobId.isValid(id)).toBe(true);
    });

    test('should return false for invalid ID', () => {
      expect(jobId.isValid('invalid-id')).toBe(false);
      expect(jobId.isValid('job:test')).toBe(false);
      expect(jobId.isValid('invalid-prefix:test:123')).toBe(false);
    });
  });

  describe('getUuid', () => {
    test('should extract UUID from valid ID', () => {
      const id = jobId.generate('test-job');
      const extractedUuid = jobId.getUuid(id);

      expect(extractedUuid).toMatch(/^[a-f0-9-]+$/);
    });

    test('should throw error for invalid ID', () => {
      expect(() => jobId.getUuid('invalid-id')).toThrow(
        'Invalid job ID format',
      );
    });
  });

  describe('fromComponents', () => {
    test('should create valid ID from components with provided UUID', () => {
      const testUuid = uuid();
      const id = jobId.fromComponents('test-job', testUuid);

      expect(id).toBe(`job:test-job:${testUuid}`);
      expect(jobId.isValid(id)).toBe(true);
    });

    test('should create valid ID from components without UUID', () => {
      const id = jobId.fromComponents('test-job');

      expect(id).toMatch(/^job:test-job:[a-f0-9-]+$/);
      expect(jobId.isValid(id)).toBe(true);
    });
  });

  describe('custom configuration', () => {
    test('should work with custom separator and prefix', () => {
      const customJobId = new JobId('|', 'task');
      const id = customJobId.generate('test-job');

      expect(id).toMatch(/^task\|test-job\|[a-f0-9-]+$/);
    });
  });
});
