package com.litedb.cluster;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * TxnLog — the coordinator's durable transaction log for 2PC recovery.
 *
 * The coordinator writes a record at "preparing" (before PREPARE — undecided) and at "committing"
 * (once every participant voted YES — this fsync is the COMMIT POINT), and deletes it once all
 * commits are acknowledged. On restart a node sweeps this log and finishes any in-doubt transaction
 * it was coordinating: "committing" → re-send COMMIT (idempotent); "preparing" → ABORT.
 *
 * Resolves coordinator failure (participants assumed alive). Participant-side restart recovery is a
 * further step — see ROADMAP.
 */
public final class TxnLog {
    private final File dir;

    public TxnLog(String dataDir) {
        this.dir = new File(dataDir, "txnlog");
        this.dir.mkdirs();
    }

    private File path(String txnId) {
        return new File(dir, txnId + ".json");
    }

    public void write(String txnId, Map<String, Object> record) {
        try {
            File tmp = new File(dir, txnId + ".json.tmp");
            try (FileOutputStream fos = new FileOutputStream(tmp)) {
                fos.write(Json.encode(record).getBytes(StandardCharsets.UTF_8));
                fos.flush();
                fos.getFD().sync();
            }
            Files.move(tmp.toPath(), path(txnId).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("txnlog write failed", e);
        }
    }

    public void remove(String txnId) {
        path(txnId).delete();
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> pending() {
        List<Map<String, Object>> out = new ArrayList<>();
        File[] files = dir.listFiles((d, n) -> n.endsWith(".json"));
        if (files != null) {
            Arrays.sort(files);
            for (File f : files) {
                try {
                    String s = new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
                    out.add((Map<String, Object>) Json.parse(s));
                } catch (IOException ignored) {
                }
            }
        }
        return out;
    }
}
