export type ConflictEventType = "replay.duplicate-operation" | "lww.tie-break";

export interface ReplayDuplicateConflictEvent {
  type: "replay.duplicate-operation";
  opId: string;
}

export interface LWWTieBreakConflictEvent {
  type: "lww.tie-break";
  winnerActorId: string;
  loserActorId: string;
}

export type ConflictEvent = ReplayDuplicateConflictEvent | LWWTieBreakConflictEvent;

export type ConflictEventListener = (event: ConflictEvent) => void;

export class ConflictEventStream {
  private readonly listeners = new Set<ConflictEventListener>();

  subscribe(listener: ConflictEventListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  emit(event: ConflictEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }
}
