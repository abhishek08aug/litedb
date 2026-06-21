package com.litedb.mvcc;

import com.litedb.lsm.LSMEngine;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * MVCCDemo — snapshot isolation, conflict detection, tombstone deletes, and GC over a real engine.
 */
public final class MVCCDemo {

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("litedb_mvcc_demo_");
        LSMEngine engine = new LSMEngine(dir.toString());
        MVCCEngine mvcc = new MVCCEngine(engine);

        // seed
        Transaction seed = mvcc.begin();
        seed.put("alice", "1000");
        seed.put("bob", "500");
        seed.commit();

        line("1) SNAPSHOT ISOLATION — read an old version while an update commits");
        Transaction txA = mvcc.begin();
        Transaction txB = mvcc.begin();                       // both snapshot the seed
        System.out.println("  txB.get(alice) before txA commits = " + txB.get("alice"));
        txA.put("alice", "800");
        txA.put("bob", "700");
        long c = txA.commit();
        System.out.println("  txA committed (alice 1000->800, bob 500->700) at ts=" + c);
        System.out.println("  txB.get(alice) AFTER txA commit       = " + txB.get("alice")
                + "   <- still its snapshot");
        System.out.println("  txB.get(bob)                          = " + txB.get("bob"));
        Transaction txC = mvcc.begin();                       // new snapshot
        System.out.println("  txC.get(alice) (fresh snapshot)       = " + txC.get("alice")
                + "    <- sees the commit");
        txB.rollback();
        txC.rollback();

        line("2) CONFLICT DETECTION — concurrent writes can't lose an update");
        Transaction t1 = mvcc.begin();
        Transaction t2 = mvcc.begin();                        // same snapshot
        t1.put("alice", "900");
        System.out.println("  t1 commit -> ts=" + t1.commit());
        t2.put("alice", "1234");                              // based on the stale snapshot
        try {
            t2.commit();
            System.out.println("  t2 commit -> (unexpected: should have conflicted)");
        } catch (ConflictException ex) {
            System.out.println("  t2 commit -> ABORTED: " + ex.getMessage());
        }

        line("3) DELETE (tombstone) — old snapshots still see the value");
        Transaction before = mvcc.begin();                    // snapshots alice=900
        Transaction del = mvcc.begin();
        del.delete("alice");
        del.commit();
        System.out.println("  after delete, fresh read alice      = " + mvcc.begin().get("alice"));
        System.out.println("  pre-delete snapshot reads alice     = " + before.get("alice"));
        before.rollback();

        line("4) GARBAGE COLLECTION — reclaim versions no snapshot can see");
        System.out.println("  versions stored before vacuum       = " + mvcc.versionCount());
        int collected = mvcc.vacuum(mvcc.lastCommitTs());     // no active old snapshots now
        System.out.println("  vacuum collected                    = " + collected + " old versions");
        System.out.println("  versions stored after vacuum        = " + mvcc.versionCount());

        engine.close();

        line("5) PERSISTENCE — reopen, timestamp + data recovered");
        LSMEngine e2 = new LSMEngine(dir.toString());
        MVCCEngine m2 = new MVCCEngine(e2);
        System.out.println("  recovered lastCommitTs              = " + m2.lastCommitTs());
        System.out.println("  fresh read alice                    = " + m2.begin().get("alice")
                + "  (deleted)");
        System.out.println("  fresh read bob                      = " + m2.begin().get("bob"));
        e2.close();

        Files.walk(dir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        System.out.println("\n[MVCC demo complete]");
    }

    private static void line(String s) {
        System.out.println("\n" + s);
        System.out.println("-".repeat(s.length()));
    }
}
