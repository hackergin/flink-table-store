package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.action.DeleteAction;

import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.table.procedures.Procedure;

import java.util.Collections;

/** delete procedure. */
public class DeleteProcedure implements Procedure {
    public String[] call(ProcedureContext context, String... args) {
        DeleteAction action =
                new DeleteAction(
                        context.getExecutionEnvironment(),
                        args[0],
                        args[1],
                        args[2],
                        args[3],
                        Collections.EMPTY_MAP);
        try {
            action.run();
            return new String[] {"call compact action success"};
        } catch (Exception e) {
            return new String[] {"call compact action failed"};
        }
    }
}
