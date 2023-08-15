package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.action.CompactAction;

import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.table.procedures.Procedure;

/** compact. */
public class CompactProcedure implements Procedure {
    public String[] call(ProcedureContext context, String... args) {
        CompactAction compactAction =
                new CompactAction(context.getExecutionEnvironment(), args[0], args[1], args[2]);
        try {
            compactAction.run();
            return new String[] {"call compact action success"};
        } catch (Exception e) {
            return new String[] {"call compact action failed"};
        }
    }
}
