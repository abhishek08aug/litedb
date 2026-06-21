package com.litedb.lsm;

import com.litedb.engine.WriteOp;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

/**
 * AtomicityDemo — shows that {@link LSMEngine#writeBatch} is all-or-nothing across a crash.
 *
 * A batch is framed in the WAL between BEGIN and COMMIT markers. On recovery a batch is applied
 * only if its COMMIT is present; a batch logged without a COMMIT (a crash mid-write) is discarded
 * entirely. This is what keeps a row and its index entries from ever diverging.
 */
public final class AtomicityDemo {

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("litedb_atomic_demo_");
        System.out.println("=== Atomic write batches (WAL BEGIN/COMMIT) ===\n");

        // Engine 1: commit one batch, then log an UNCOMMITTED batch and "crash".
        LSMEngine e1 = new LSMEngine(dir.toString());
        e1.writeBatch(List.of(WriteOp.put("a", "1"), WriteOp.put("b", "2")));
        System.out.println("committed batch:                       a=1, b=2");
        e1.writeBatchSimulateCrash(List.of(WriteOp.put("c", "3"), WriteOp.put("d", "4")));
        System.out.println("logged but NOT committed (crash sim):  c=3, d=4");
        System.out.println("...crash: no clean close, MemTable lost, WAL kept (fsync'd)\n");
        // Deliberately do NOT call e1.close().

        // Engine 2: recover from the same data dir.
        LSMEngine e2 = new LSMEngine(dir.toString());
        System.out.println("after recovery:");
        System.out.println("  GET a = " + e2.get("a") + "   (committed   -> present)");
        System.out.println("  GET b = " + e2.get("b") + "   (committed   -> present)");
        System.out.println("  GET c = " + e2.get("c") + " (uncommitted -> discarded)");
        System.out.println("  GET d = " + e2.get("d") + " (uncommitted -> discarded)");
        e2.close();

        Files.walk(dir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        System.out.println("\n[Atomicity demo complete] — the batch committed all-or-nothing.");
    }
}
