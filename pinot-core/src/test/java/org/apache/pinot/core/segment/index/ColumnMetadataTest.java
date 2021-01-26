/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.segment.index;

import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segments.v1.creator.SegmentTestUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ColumnMetadataTest {
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "ColumnMetadataTest");
  private static final String CREATOR_VERSION = "TestHadoopJar.1.1.1";

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  public SegmentGeneratorConfig CreateSegmentConfigWithoutCreator()
      throws Exception {
    final String filePath =
        TestUtils.getFileFromResourceUrl(ColumnMetadataTest.class.getClassLoader().getResource(AVRO_DATA));
    // Intentionally changed this to TimeUnit.Hours to make it non-default for testing.
    SegmentGeneratorConfig config = SegmentTestUtils
        .getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch", TimeUnit.HOURS,
            "testTable");
    config.setSegmentNamePostfix("1");
    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    config.setSkipTimeValueCheck(true);
    return config;
  }

  public SegmentGeneratorConfig createSegmentConfigWithCreator()
      throws Exception {
    SegmentGeneratorConfig config = CreateSegmentConfigWithoutCreator();
    config.setCreatorVersion(CREATOR_VERSION);
    return config;
  }

  public void verifySegmentAfterLoading(SegmentMetadataImpl metadata) {
    // Multi-value numeric dimension column.
    ColumnMetadata col7Meta = metadata.getColumnMetadataFor("column7");
    Assert.assertEquals(col7Meta.getColumnName(), "column7");
    Assert.assertEquals(col7Meta.getCardinality(), 359);
    Assert.assertEquals(col7Meta.getTotalDocs(), 100000);
    Assert.assertEquals(col7Meta.getDataType(), FieldSpec.DataType.INT);
    Assert.assertEquals(col7Meta.getBitsPerElement(), 9);
    Assert.assertEquals(col7Meta.getColumnMaxLength(), 0);
    Assert.assertEquals(col7Meta.getFieldType(), FieldSpec.FieldType.DIMENSION);
    Assert.assertFalse(col7Meta.isSorted());
    Assert.assertFalse(col7Meta.hasNulls());
    Assert.assertTrue(col7Meta.hasDictionary());
    Assert.assertTrue(col7Meta.hasInvertedIndex());
    Assert.assertFalse(col7Meta.isSingleValue());
    Assert.assertEquals(col7Meta.getMaxNumberOfMultiValues(), 24);
    Assert.assertEquals(col7Meta.getTotalNumberOfEntries(), 134090);
    Assert.assertFalse(col7Meta.isAutoGenerated());
    Assert.assertEquals(col7Meta.getDefaultNullValueString(), String.valueOf(Integer.MIN_VALUE));

    // Single-value string dimension column.
    ColumnMetadata col3Meta = metadata.getColumnMetadataFor("column3");
    Assert.assertEquals(col3Meta.getColumnName(), "column3");
    Assert.assertEquals(col3Meta.getCardinality(), 5);
    Assert.assertEquals(col3Meta.getTotalDocs(), 100000);
    Assert.assertEquals(col3Meta.getDataType(), FieldSpec.DataType.STRING);
    Assert.assertEquals(col3Meta.getBitsPerElement(), 3);
    Assert.assertEquals(col3Meta.getColumnMaxLength(), 4);
    Assert.assertEquals(col3Meta.getFieldType(), FieldSpec.FieldType.DIMENSION);
    Assert.assertFalse(col3Meta.isSorted());
    Assert.assertFalse(col3Meta.hasNulls());
    Assert.assertTrue(col3Meta.hasDictionary());
    Assert.assertTrue(col3Meta.hasInvertedIndex());
    Assert.assertFalse(col3Meta.hasFSTIndex());
    Assert.assertTrue(col3Meta.isSingleValue());
    Assert.assertEquals(col3Meta.getMaxNumberOfMultiValues(), 0);
    Assert.assertEquals(col3Meta.getTotalNumberOfEntries(), 100000);
    Assert.assertFalse(col3Meta.isAutoGenerated());
    Assert.assertEquals(col3Meta.getDefaultNullValueString(), "null");

    // Time column.
    ColumnMetadata timeColumn = metadata.getColumnMetadataFor("daysSinceEpoch");
    Assert.assertEquals(timeColumn.getColumnName(), "daysSinceEpoch");
    Assert.assertEquals(timeColumn.getCardinality(), 1);
    Assert.assertEquals(timeColumn.getTotalDocs(), 100000);
    Assert.assertEquals(timeColumn.getDataType(), FieldSpec.DataType.INT);
    Assert.assertEquals(timeColumn.getBitsPerElement(), 1);
    Assert.assertEquals(timeColumn.getColumnMaxLength(), 0);
    Assert.assertEquals(timeColumn.getFieldType(), FieldSpec.FieldType.DIMENSION);
    Assert.assertTrue(timeColumn.isSorted());
    Assert.assertFalse(timeColumn.hasNulls());
    Assert.assertTrue(timeColumn.hasDictionary());
    Assert.assertTrue(timeColumn.hasInvertedIndex());
    Assert.assertTrue(timeColumn.isSingleValue());
    Assert.assertEquals(timeColumn.getMaxNumberOfMultiValues(), 0);
    Assert.assertEquals(timeColumn.getTotalNumberOfEntries(), 100000);
    Assert.assertFalse(timeColumn.isAutoGenerated());
    Assert.assertEquals(timeColumn.getDefaultNullValueString(), String.valueOf(Integer.MIN_VALUE));
  }

  @Test
  public void testAllFieldsInitialized()
      throws Exception {
    // Build the Segment metadata.
    SegmentGeneratorConfig config = createSegmentConfigWithCreator();
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    // Load segment metadata.
    IndexSegment segment = ImmutableSegmentLoader.load(INDEX_DIR.listFiles()[0], ReadMode.mmap);
    SegmentMetadataImpl metadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
    verifySegmentAfterLoading(metadata);

    // Make sure we got the creator name as well.
    String creatorName = metadata.getCreatorName();
    Assert.assertEquals(creatorName, CREATOR_VERSION);
  }

  @Test
  public void testAllFieldsExceptCreatorName()
      throws Exception {
    // Build the Segment metadata.
    SegmentGeneratorConfig config = CreateSegmentConfigWithoutCreator();
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    // Load segment metadata.
    IndexSegment segment = ImmutableSegmentLoader.load(INDEX_DIR.listFiles()[0], ReadMode.mmap);
    SegmentMetadataImpl metadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
    verifySegmentAfterLoading(metadata);

    // Make sure we get null for creator name.
    String creatorName = metadata.getCreatorName();
    Assert.assertEquals(creatorName, null);
  }

  @Test
  public void testPaddingCharacter()
      throws Exception {
    // Build the Segment metadata.
    SegmentGeneratorConfig config = CreateSegmentConfigWithoutCreator();
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    // Load segment metadata.
    IndexSegment segment = ImmutableSegmentLoader.load(INDEX_DIR.listFiles()[0], ReadMode.mmap);
    SegmentMetadataImpl metadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
    verifySegmentAfterLoading(metadata);

    // Make sure we get null for creator name.
    char paddingCharacter = metadata.getPaddingCharacter();
    Assert.assertEquals(paddingCharacter, '\0');
  }
}

