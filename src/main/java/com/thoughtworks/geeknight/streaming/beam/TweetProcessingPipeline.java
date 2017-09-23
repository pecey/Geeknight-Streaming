package com.thoughtworks.geeknight.streaming.beam;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thoughtworks.geeknight.streaming.kafka.StatusDeserializer;
import com.thoughtworks.geeknight.streaming.kafka.StatusWrapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;


public class TweetProcessingPipeline {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(KafkaIO.<String, StatusWrapper>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("test")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializerAndCoder(StatusDeserializer.class, StatusCoder.of())
        )
                .apply(ParDo.of(new DoFn<KafkaRecord<String, StatusWrapper>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext context){
                System.out.println(context.element().getKV().getValue().getLanguage());
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}

class StatusCoder extends Coder<StatusWrapper> {

    private static final StatusCoder INSTANCE = new StatusCoder();
    public static StatusCoder of(){
        return  INSTANCE;
    }
    @Override
    public void encode(StatusWrapper statusWrapper, OutputStream outputStream) throws CoderException, IOException {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(statusWrapper).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        outputStream.write(retVal);
    }

    @Override
    public StatusWrapper decode(InputStream inputStream) throws CoderException, IOException {
        byte[] input = StreamUtils.getBytes(inputStream);
        ObjectMapper mapper = new ObjectMapper();
        StatusWrapper status = null;
        try {
            status = mapper.readValue(input, StatusWrapper.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return status;
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {

    }
}
