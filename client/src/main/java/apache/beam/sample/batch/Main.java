package apache.beam.sample.batch;

import apache.beam.sample.FormatOutput;
import apache.beam.sample.WordCount;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class Main {

    public static void main(String[] args) {
        String inputFilePattern = "data/*";
        String outputsPrefix = "outputs/batch/part";

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> files = pipeline.apply("Read input", TextIO.read().from(inputFilePattern));

        PCollection<KV<String, Long>> wordCount = files.apply("Count words", new WordCount());

        PCollection<String> output = wordCount.apply("Format data", new FormatOutput());

        output.apply("Write output", TextIO.write().to(outputsPrefix));
        pipeline.run().waitUntilFinish();
    }

}
