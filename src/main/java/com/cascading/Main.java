
package com.cascading;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.Properties;

/**
 * A Cascading example to read a text file and convert the string to upper case letters and write into the output file
 */
public class Main {

    public static void main(String[] args) {

        //input and output path
        String inputPath = args[0];
        String outputPath = args[1];
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Main.class);
        //Create the source tap
        Tap inTap = new Hfs(new TextDelimited(new Fields("line"), true, "\t"), inputPath);
        //Create the sink tap
        Tap outTap = new Hfs(new TextDelimited(false, "\t"), outputPath, SinkMode.REPLACE);

        // Pipe to connect Source and Sink Tap
        Pipe wordsPipe = new Each("words", new UpperCaseFunction(new Fields("line")));
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);
        Flow flow = flowConnector.connect("Hdfs Job", inTap, outTap, wordsPipe);
        flow.complete();
    }

    public static class UpperCaseFunction extends BaseOperation implements Function {
        private static final long serialVersionUID = 1L;

        public UpperCaseFunction(Fields fields) {
            super(1, fields);
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            TupleEntry arguments = functionCall.getArguments();
            Tuple tuple = new Tuple();
            if (arguments == null || arguments.getString(0) == null) {
                return;
            }
            String original = arguments.getString(0).trim();
            tuple.add(original.toUpperCase());
            functionCall.getOutputCollector().add(tuple);
        }
    }
}

