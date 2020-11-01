package apache.beam.sample;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class PrintIO extends DoFn<String, String> {

    @ProcessElement
    public void process(@Element String string, OutputReceiver<String> out) {
        System.out.println(string);

        out.output(string);
    }

}
