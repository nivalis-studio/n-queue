/**
 * Sleep for a specified duration
 * @param {number} ms - The duration in milliseconds
 * @returns {Promise<void>}
 * @private
 */
export const sleep = async (ms: number): Promise<void> => {
  await new Promise(resolve => {
    setTimeout(resolve, ms);
  });
};
