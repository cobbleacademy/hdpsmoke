package com.hsm.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.SparkSessionExtensionsProvider;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo;
import org.apache.spark.sql.classic.ColumnNodeToExpressionConverter$;
import org.apache.spark.sql.classic.ExpressionUtils;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;
import scala.Tuple3;
import scala.collection.Iterator;
import scala.collection.immutable.Seq;
import scala.runtime.BoxedUnit;

/**
 * Registers {@code hsm_encrypt}/{@code hsm_decrypt} automatically for every
 * {@code SparkSession} created in a cluster configured with:
 * <pre>
 *   spark.sql.extensions=com.hsm.spark.HsmUdfExtension
 * </pre>
 * No per-application registration code needed -- see {@link HsmUdfRegistration}
 * for the explicit, per-application alternative built on the same two UDF
 * classes.
 *
 * <p><b>Instantiation contract, verified against real Spark bytecode, not
 * documentation:</b> {@code SparkSession}'s {@code applyExtensions} reflects
 * on the configured class name with {@code Class.getConstructor()} (a
 * no-arg constructor -- required), then casts the instance to
 * {@code scala.Function1} and calls {@code apply(extensions)} on it.
 * Confirmed byte-for-byte identical between Spark 3.5.1 and 4.2.0 by
 * disassembling both jars' {@code SparkSession$.$anonfun$applyExtensions$2}
 * -- this is not a version-specific detail. This class implements
 * {@link SparkSessionExtensionsProvider} (a plain {@code Function1<
 * SparkSessionExtensions, BoxedUnit>} marker interface Spark itself
 * provides for exactly this purpose) instead of doing registration inside
 * a constructor that takes {@code SparkSessionExtensions} directly --
 * a constructor-injection shape that looks natural but Spark's reflection
 * code never actually calls, on either version.
 *
 * <p><b>Version sensitivity, read before deploying:</b> unlike
 * {@link HsmUdfRegistration} (built entirely on the stable, public
 * {@code UDFRegistration} API), this class reaches into Catalyst-internal
 * APIs ({@code SparkSessionExtensions.injectFunction}, {@code ExpressionInfo}'s
 * constructor shape, and -- since Spark 4.x's Classic/Connect API
 * unification replaced {@code Column}'s {@code Expression} backing with a
 * {@code ColumnNode} one -- {@code org.apache.spark.sql.classic.ExpressionUtils}/
 * {@code ColumnNodeToExpressionConverter}) that are not guaranteed source- or
 * binary-stable across Spark releases the way {@code UDF1}/{@code UDF3} are.
 * Verified to compile AND to actually run -- a real local
 * {@code SparkSession} with this extension configured, registering both
 * functions and executing {@code hsm_decrypt(...)} through the full
 * Catalyst bridge, including real whole-stage codegen (not just interpreted
 * eval) -- against {@code spark-sql_2.13:4.2.0} (this module's
 * {@code pom.xml} pin). Two real bugs surfaced only by that live run, never
 * by {@code mvn compile}: (1) this class originally took
 * {@code SparkSessionExtensions} as a constructor argument, which compiles
 * fine but Spark's reflection code never actually calls -- see the
 * instantiation-contract note above; (2) converting the registered
 * function's result {@code Column} back to an {@code Expression} via
 * {@code ExpressionUtils.expression(...)} compiles fine but produces an
 * {@code Unevaluable ColumnNodeExpression} wrapper that throws
 * {@code [INTERNAL_ERROR] Cannot generate code for expression} the moment
 * Spark tries to codegen it -- {@code ExpressionUtils.expression} is meant
 * for use inside Spark's own Column pipeline, where a later Analyzer rule
 * resolves that wrapper away; code injected directly via
 * {@code injectFunction} runs after that rule already had its chance, so it
 * never gets resolved. {@code ColumnNodeToExpressionConverter} (below) does
 * that resolution immediately instead, which is what actually works.
 * Re-verify (or fall back to {@link HsmUdfRegistration}, which carries none
 * of this risk) if the target cluster runs a materially different Spark
 * version. In particular, the {@code Column}/{@code Expression} bridging in
 * this file does NOT compile against Spark 3.5.1 -- that version's
 * {@code Column} has a direct {@code Column(Expression)} constructor and no
 * {@code classic.ExpressionUtils}/{@code ColumnNodeToExpressionConverter}
 * classes; a 3.x target needs the pre-4.x form of this file (see git
 * history) instead, though the {@code SparkSessionExtensionsProvider}
 * instantiation shape stays the same either way (confirmed identical in
 * both versions' bytecode).
 *
 * <p>Bridges a Java {@code UDF1}/{@code UDF3} into a raw Catalyst
 * {@code Expression} via {@code functions.udf(...)} (producing a
 * {@link UserDefinedFunction}) rather than hand-rolling an {@code Expression}
 * subclass (its own {@code eval}/codegen) -- reuses everything Spark's own
 * {@code UDFRegistration.register(...)} already does internally for type
 * handling, so this only has to bridge {@code Seq<Expression>} to
 * {@code Column[]} (via {@code ExpressionUtils.column}, wrapping each
 * argument {@code Expression} as a {@code ColumnNode}) and the result
 * {@code Column} back to a real, codegen-capable {@code Expression} (via
 * {@code ColumnNodeToExpressionConverter}, not {@code ExpressionUtils.expression}
 * -- see above).
 */
public class HsmUdfExtension implements SparkSessionExtensionsProvider {

    public HsmUdfExtension() {
    }

    @Override
    public BoxedUnit apply(SparkSessionExtensions extensions) {
        registerFunction(extensions, HsmUdfRegistration.ENCRYPT_FUNCTION_NAME,
                functions.udf(new HsmEncryptUdf(), DataTypes.StringType));
        registerFunction(extensions, HsmUdfRegistration.DECRYPT_FUNCTION_NAME,
                functions.udf(new HsmDecryptUdf(), DataTypes.StringType));
        return BoxedUnit.UNIT;
    }

    private static void registerFunction(SparkSessionExtensions extensions, String name, UserDefinedFunction udf) {
        FunctionIdentifier id = FunctionIdentifier.apply(name);
        ExpressionInfo info = new ExpressionInfo(HsmUdfExtension.class.getName(), name);
        Function1<Seq<Expression>, Expression> builder =
                args -> ColumnNodeToExpressionConverter$.MODULE$.apply(udf.apply(toColumns(args)).node());
        extensions.injectFunction(new Tuple3<>(id, info, builder));
    }

    private static Column[] toColumns(Seq<Expression> args) {
        Column[] columns = new Column[args.length()];
        Iterator<Expression> it = args.iterator();
        int i = 0;
        while (it.hasNext()) {
            columns[i++] = ExpressionUtils.column(it.next());
        }
        return columns;
    }
}
