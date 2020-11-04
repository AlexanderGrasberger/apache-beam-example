package apache.beam.sample.stream;

import apache.beam.sample.WordCount;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        String inputFilePattern = "data/*";
        String outputsPrefix = "outputs/stream/";

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KafkaRecord<Long, String>> topics = pipeline.apply("Read input", KafkaIO.<Long, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("text_topic")
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
        );
        PCollection<KafkaRecord<Long, String>> windowed = topics.apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))));

        PCollection<String> text = windowed.apply(ParDo.of(new DoFn<KafkaRecord<Long, String>, String>() {
            @ProcessElement
            public void processElement(@Element KafkaRecord<Long, String> element, OutputReceiver<String> out) {
                out.output(element.getKV().getValue());
            }
        }));

        PCollection<KV<String, Long>> wordCount = text.apply("Count words", new WordCount());

        wordCount.apply("Write output", KafkaIO.<String, Long>write()
                .withBootstrapServers("localhost:9092")
                .withTopic("word_count_topic")
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(LongSerializer.class));
        pipeline.run().waitUntilFinish();
    }

}
