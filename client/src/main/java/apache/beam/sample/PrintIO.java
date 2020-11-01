package apache.beam.sample;

import org.apache.beam.sdk.transforms.DoFn;

public class PrintIO extends DoFn<String, String> {

    @ProcessElement
    public void process(@Element String string, OutputReceiver<String> out) {
        System.out.println(string);

        out.output(string);
    }

}
