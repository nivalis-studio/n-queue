import { Effect } from 'effect';

export const runEffect = async <A, E = never>(
  effect: Effect.Effect<A, E>,
): Promise<A> => {
  return await Effect.runPromise(effect);
};
