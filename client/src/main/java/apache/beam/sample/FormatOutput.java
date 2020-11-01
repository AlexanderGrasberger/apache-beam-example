package apache.beam.sample;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;

public class FormatOutput extends PTransform<PCollection<KV<String, Long>>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<KV<String, Long>> input) {
        return input.apply(MapElements.into(TypeDescriptors.strings())
                .via((KV<String, Long> group) -> group.getKey() + ": " + group.getValue()));
    }
}
