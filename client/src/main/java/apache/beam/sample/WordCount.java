package apache.beam.sample;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class WordCount extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> input) {
        return input
                .apply("Tokenize", FlatMapElements.into(TypeDescriptors.strings())
                        .via((String line) -> Arrays.asList(line.split("\\s")))
                )
                .apply("Count words", Count.perElement());
    }
}