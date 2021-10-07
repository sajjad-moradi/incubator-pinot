package org.apache.pinot.integration.tests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MultiStreamConsumptionIntegrationTest extends BaseClusterIntegrationTest {

  // flights
  public static final String FLIGHTS = "Flights";
  public static final String FLIGHT_NUM = "flightNum";
  public static final String SOURCE_CITY = "sourceCity";
  public static final String DEST_CITY = "destCity";
  public static final String DEPARTURE_TIME = "departureTime";
  public static final String ARRIVAL_TIME = "arrivalTime";
  public static final String AIRLINE = "airline";
  public static final String CREATION_DATE = "creationDate";

  // trains
  public static final String TRAIN_SCHEDULES = "TrainSchedules";
  public static final String TRAIN_NO = "trainNo";
  public static final String SRC = "src";
  public static final String DST = "dst";
  public static final String DEP_TIME = "depTime";
  public static final String ARR_TIME = "arrTime";
  public static final String OPERATOR = "operator";
  public static final String CREATE_DATE = "createDate";

  // transport
  public static final String TRANSPORT_SCHEDULE = "transportSchedule";
  public static final String SCHEDULE_NO = "scheduleNo";
  public static final String TYPE = "type";
  public static final String SOURCE = "source";
  public static final String DESTINATION = "destination";

  static org.apache.avro.Schema _flightsSchema =
      SchemaBuilder.record(FLIGHTS).fields()
          .requiredInt(FLIGHT_NUM)
          .requiredString(SOURCE_CITY)
          .requiredString(DEST_CITY)
          .requiredLong(DEPARTURE_TIME)
          .requiredLong(ARRIVAL_TIME)
          .requiredString(AIRLINE)
          .requiredInt(CREATION_DATE)
          .endRecord();

  static org.apache.avro.Schema _trainScheduleSchema =
      SchemaBuilder.record(TRAIN_SCHEDULES).fields()
          .requiredInt(TRAIN_NO)
          .requiredString(SRC)
          .requiredString(DST)
          .requiredLong(DEP_TIME)
          .requiredLong(ARR_TIME)
          .requiredString(OPERATOR)
          .requiredInt(CREATE_DATE)
          .endRecord();


  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    startKafkaWithTopics(FLIGHTS, 2, TRAIN_SCHEDULES, 2);

    // add table schema
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName(TRANSPORT_SCHEDULE)
        .addSingleValueDimension(SCHEDULE_NO, FieldSpec.DataType.INT)
        .addSingleValueDimension(TYPE, FieldSpec.DataType.STRING)
        .addSingleValueDimension(SOURCE, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DESTINATION, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DEPARTURE_TIME, FieldSpec.DataType.LONG)
        .addSingleValueDimension(ARRIVAL_TIME, FieldSpec.DataType.LONG)
        .addSingleValueDimension(OPERATOR, FieldSpec.DataType.STRING)
        .addDateTime(CREATE_DATE, FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .build();
    addSchema(schema);

    // add table config
    StreamIngestionConfig streamIngestionConfig = new StreamIngestionConfig(Arrays.asList(
        getFlightStreamIngestionConfigs(),
        getTrainStreamIngestionConfigs()
    ));
    List<TransformConfig> transformConfigs = Arrays.asList(
        //                  column in pinot table..........................column in avro record....stream name
        new TransformConfig(SCHEDULE_NO, String.format("Groovy({%s}, %s)", FLIGHT_NUM, FLIGHT_NUM), FLIGHTS),
        new TransformConfig(SCHEDULE_NO, String.format("Groovy({%s}, %s)", TRAIN_NO, TRAIN_NO), TRAIN_SCHEDULES),
        new TransformConfig(TYPE, String.format("Groovy({\"flight\"}, %s)", FLIGHT_NUM), FLIGHTS), // any better way?
        new TransformConfig(TYPE, String.format("Groovy({\"train\"}, %s)", TRAIN_NO), TRAIN_SCHEDULES),
        new TransformConfig(SOURCE, String.format("Groovy({%s}, %s)", SOURCE_CITY, SOURCE_CITY), FLIGHTS),
        new TransformConfig(SOURCE, String.format("Groovy({%s}, %s)", SRC, SRC), TRAIN_SCHEDULES),
        new TransformConfig(DESTINATION, String.format("Groovy({%s}, %s)", DEST_CITY, DEST_CITY), FLIGHTS),
        new TransformConfig(DESTINATION, String.format("Groovy({%s}, %s)", DST, DST), TRAIN_SCHEDULES),
        new TransformConfig(DEPARTURE_TIME, String.format("Groovy({%s}, %s)", DEP_TIME, DEP_TIME), TRAIN_SCHEDULES),
        new TransformConfig(ARRIVAL_TIME, String.format("Groovy({%s}, %s)", ARR_TIME, ARR_TIME), TRAIN_SCHEDULES),
        new TransformConfig(OPERATOR, String.format("Groovy({%s}, %s)", AIRLINE, AIRLINE), FLIGHTS),
        new TransformConfig(CREATE_DATE, String.format("Groovy({%s}, %s)", CREATION_DATE, CREATION_DATE), FLIGHTS)
    );
    IngestionConfig ingestionConfig = new IngestionConfig(null, streamIngestionConfig, null, transformConfigs, null);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TRANSPORT_SCHEDULE)
        .setSchemaName(TRANSPORT_SCHEDULE)
        .setTimeColumnName(CREATE_DATE)
        .setNumReplicas(getNumReplicas())
        .setLLC(true)
        .setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode())
        .setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant())
        .setIngestionConfig(ingestionConfig)
        .build();
    addTableConfig(tableConfig);

    // push data
    pushDataIntoKafka();

    waitForAllDocsLoaded(3_000_000);
    System.out.println("wait here");
  }

  @Override
  protected String getTableName() {
    return TRANSPORT_SCHEDULE;
  }

  @Override
  protected long getCountStarResult() {
    return 16;
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void t() {
    System.out.println("what?!");
  }

  private Map<String, String> getTrainStreamIngestionConfigs() {
    String streamType = "kafka";
    return new ImmutableMap.Builder<String, String>()
        .put(StreamConfigProperties.STREAM_TYPE, streamType)
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            StreamConfig.ConsumerType.LOWLEVEL.toString())
        .put(KafkaStreamConfigProperties
                .constructStreamProperty(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BROKER_LIST),
            "localhost:" + _kafkaStarters.get(1).getPort())
        .put(StreamConfigProperties
                .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
            getStreamConsumerFactoryClassName())
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
            TRAIN_SCHEDULES)
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
            TrainSchedulesMessageDecoder.class.getName())
        .put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "6")
        .put(StreamConfigProperties
            .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), "smallest")
        .build();
  }

  private ImmutableMap<String, String> getFlightStreamIngestionConfigs() {
    String streamType = "kafka";
    return new ImmutableMap.Builder<String, String>()
        .put(StreamConfigProperties.STREAM_TYPE, streamType)
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            StreamConfig.ConsumerType.LOWLEVEL.toString())
        .put(KafkaStreamConfigProperties
                .constructStreamProperty(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BROKER_LIST),
            "localhost:" + _kafkaStarters.get(0).getPort())
        .put(StreamConfigProperties
                .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
            getStreamConsumerFactoryClassName())
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
            FLIGHTS)
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
            FlightsMessageDecoder.class.getName())
        .put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "6")
        .put(StreamConfigProperties
                .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA),
            "smallest")
        .build();
  }

  private void startKafkaWithTopics(String topic1, int numPartitions1, String topic2, int numPartitions2) {
    Properties kafkaConfig = KafkaStarterUtils.getDefaultKafkaConfiguration();
    int kafkaPort = KafkaStarterUtils.DEFAULT_KAFKA_PORT;
    _kafkaStarters = KafkaStarterUtils.startServers(2, kafkaPort, getKafkaZKAddress(), kafkaConfig);
    _kafkaStarters.get(0).createTopic(topic1, KafkaStarterUtils.getTopicCreationProps(numPartitions1));
    _kafkaStarters.get(0).createTopic(topic2, KafkaStarterUtils.getTopicCreationProps(numPartitions2));
  }

  private void pushDataIntoKafka()
      throws Exception {

    // flights
    File flightsAvroFile = new File(FLIGHTS + ".avro"); // use temp file that gets auto-deleted
    DataFileWriter<GenericRecord> flightsAvroWriter = new DataFileWriter<>(new GenericDatumWriter<>(_flightsSchema));
    flightsAvroWriter.create(_flightsSchema, flightsAvroFile);
    addData(FLIGHTS, flightsAvroWriter, 1, "San Fransisco", "San Diego",  101L, 102L, 1, "AA");
    addData(FLIGHTS, flightsAvroWriter, 2, "San Fransisco", "Los Angles", 201L, 202L, 2, "Delta");
    addData(FLIGHTS, flightsAvroWriter, 3, "San Fransisco", "Phoenix",    301L, 302L, 3, "Southwest");
    addData(FLIGHTS, flightsAvroWriter, 4, "San Jose",      "Seattle",    401L, 402L, 4, "Alaska");
    addData(FLIGHTS, flightsAvroWriter, 5, "San Jose",      "New York",   501L, 502L, 5, "AA");
    addData(FLIGHTS, flightsAvroWriter, 6, "San Jose",      "Dallas",     601L, 602L, 6, "Southwest");
    addData(FLIGHTS, flightsAvroWriter, 7, "Oakland",       "San Diego",  701L, 702L, 7, "AA");
    addData(FLIGHTS, flightsAvroWriter, 8, "Oakland",       "Los Angles", 801L, 802L, 8, "AA");
    flightsAvroWriter.close();

    // trains
    File trainAvroFile = new File(TRAIN_SCHEDULES + ".avro"); // use temp file that gets auto-deleted
    DataFileWriter<GenericRecord> trainAvroWriter =
        new DataFileWriter<>(new GenericDatumWriter<>(_trainScheduleSchema));
    trainAvroWriter.create(_trainScheduleSchema, trainAvroFile);
    addData(TRAIN_SCHEDULES, trainAvroWriter, 1, "San Fransisco", "Fremont",       101L, 102L, 1, "BART");
    addData(TRAIN_SCHEDULES, trainAvroWriter, 2, "San Fransisco", "Dublin",        201L, 202L, 2, "BART");
    addData(TRAIN_SCHEDULES, trainAvroWriter, 3, "San Fransisco", "Mountain View", 301L, 302L, 3, "BART");
    addData(TRAIN_SCHEDULES, trainAvroWriter, 4, "San Jose",      "Oakland",       401L, 402L, 4, "BART");
    addData(TRAIN_SCHEDULES, trainAvroWriter, 5, "Sunnyvale",     "Foster City",   501L, 502L, 5, "CalTrain");
    addData(TRAIN_SCHEDULES, trainAvroWriter, 6, "San Jose",      "San Fransisco", 601L, 602L, 6, "CalTrain");
    addData(TRAIN_SCHEDULES, trainAvroWriter, 7, "San Jose",      "Livermore",     701L, 702L, 7, "BART");
    addData(TRAIN_SCHEDULES, trainAvroWriter, 8, "Dublin",        "Oakland",       801L, 802L, 8, "BART");
    trainAvroWriter.close();

    pushAvroFileToKafka(flightsAvroFile, FLIGHTS, FLIGHT_NUM);
    pushAvroFileToKafka(trainAvroFile, TRAIN_SCHEDULES, TRAIN_NO);
  }

  private void pushAvroFileToKafka(File avroFile, String topic, String partitionColumn)
      throws Exception {
    ClusterIntegrationTestUtils.pushAvroIntoKafka(ImmutableList.of(avroFile), "localhost:" + getKafkaPort(), topic,
        getMaxNumKafkaMessagesPerBatch(), null, partitionColumn);
  }

  private void addData(String type, DataFileWriter<GenericRecord> avroFileWriter, int num, String src, String dest,
      long depTime, long arrTime, int creationDate, String operator)
      throws Exception {
    GenericRecord record;
    boolean isFlights = type.equals(FLIGHTS);
    if (isFlights) {
      record = new GenericData.Record(_flightsSchema);
      record.put(FLIGHT_NUM, num);
      record.put(SOURCE_CITY, src);
      record.put(DEST_CITY, dest);
      record.put(DEPARTURE_TIME, depTime);
      record.put(ARRIVAL_TIME, arrTime);
      record.put(AIRLINE, operator);
      record.put(CREATION_DATE, creationDate);
    } else {
      record = new GenericData.Record(_trainScheduleSchema);
      record.put(TRAIN_NO, num);
      record.put(SRC, src);
      record.put(DST, dest);
      record.put(DEP_TIME, depTime);
      record.put(ARR_TIME, arrTime);
      record.put(OPERATOR, operator);
      record.put(CREATE_DATE, creationDate);
    }
    avroFileWriter.append(record);
  }


  @Override
  protected boolean useLlc() {
    return true;
  }

  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Override
  protected List<String> getInvertedIndexColumns() {
    return null;
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return null;
  }

  @Override
  protected List<String> getRangeIndexColumns() {
    return null;
  }

  @Override
  protected List<String> getBloomFilterColumns() {
    return null;
  }

  public static class FlightsMessageDecoder implements StreamMessageDecoder<byte[]> {

    private RecordExtractor _recordExtractor;
    private DecoderFactory _decoderFactory = new DecoderFactory();
    private DatumReader<GenericData.Record> _reader;

    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
        throws Exception {
      _reader = new GenericDatumReader<>(_flightsSchema);
      _recordExtractor = new AvroRecordExtractor();
      _recordExtractor.init(fieldsToRead, null);
    }

    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      return decode(payload, 0, payload.length, destination);
    }

    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      try {
        GenericData.Record avroRecord =
            _reader.read(null, _decoderFactory.binaryDecoder(payload, offset, length, null));
        return _recordExtractor.extract(avroRecord, destination);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class TrainSchedulesMessageDecoder implements StreamMessageDecoder<byte[]> {

    private RecordExtractor _recordExtractor;
    private DecoderFactory _decoderFactory = new DecoderFactory();
    private DatumReader<GenericData.Record> _reader;

    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
        throws Exception {
      _reader = new GenericDatumReader<>(_trainScheduleSchema);
      _recordExtractor = new AvroRecordExtractor();
      _recordExtractor.init(fieldsToRead, null);
    }

    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      return decode(payload, 0, payload.length, destination);
    }

    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      try {
        GenericData.Record avroRecord =
            _reader.read(null, _decoderFactory.binaryDecoder(payload, offset, length, null));
        return _recordExtractor.extract(avroRecord, destination);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}

