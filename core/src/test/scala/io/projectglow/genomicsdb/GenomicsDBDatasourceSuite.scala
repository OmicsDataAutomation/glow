/*
 * Copyright 2021 The Glow Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.projectglow.genomicsdb

import collection.JavaConverters._
import java.nio.file.Files
import java.util.Base64

import htsjdk.variant.vcf.VCFConstants
import org.genomicsdb.model.{Coordinates, GenomicsDBExportConfiguration}
import org.genomicsdb.spark.GenomicsDBSchemaFactory
import com.googlecode.protobuf.format.JsonFormat

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.{SparkConf, SparkException}
import io.projectglow.common.{GenotypeFields, VCFRow}
import io.projectglow.sql.{GlowBaseTest, GlowConf}

import io.projectglow.functions._

object TestProto0 {

  val baseBuilder = GenomicsDBExportConfiguration.ExportConfiguration.newBuilder()
    .setWorkspace("test-data/genomicsdb/ws")
    .setArrayName("t0_1_2")
    .setReferenceGenome("test-data/genomicsdb/chr1_10MB.fasta.gz")
    .addAllAttributes(Iterable("REF", "ALT", "DP","GT").asJava)
    .setVcfHeaderFilename("test-data/genomicsdb/template_vcf_header.vcf")
    .setSegmentSize(40)

  // column intervals
  val ci1 = Coordinates.ContigInterval.newBuilder()
    .setContig("1").setBegin(0).setEnd(249250621)
  
  // interval list 
  val tileInterval = Coordinates.TileDBColumnInterval.newBuilder()
    .setBegin(0)
    .setEnd(1000000000)

  val colIntervalBase = Coordinates.GenomicsDBColumnInterval.newBuilder()
    .setTiledbColumnInterval(tileInterval)

  val colInterval = Coordinates.GenomicsDBColumnOrInterval.newBuilder()
    .setColumnInterval(colIntervalBase)

  val colRanges = GenomicsDBExportConfiguration.GenomicsDBColumnOrIntervalList.newBuilder()
    .addColumnOrIntervalList(colInterval)

  // row ranges
  val rows = GenomicsDBExportConfiguration.RowRange.newBuilder()
    .setLow(0).setHigh(3)

  val rowLists = GenomicsDBExportConfiguration.RowRangeList.newBuilder()
    .addRangeList(rows)

  val builder = baseBuilder.addQueryColumnRanges(colRanges)
    //.addQueryContigIntervals(ci1)
    .addQueryRowRanges(rowLists)
  
  println(builder.build().toString())
  val pb = builder.build().toByteArray();
  val pbQueryFile = Base64.getEncoder().encodeToString(pb);

}

object TestProto1 {

  val baseBuilder = GenomicsDBExportConfiguration.ExportConfiguration.newBuilder()
    .setWorkspace("test-data/genomicsdb/ws0")
    .setArrayName("test0")
    .setReferenceGenome("test-data/genomicsdb/chr1_10MB.fasta.gz")
    .addAllAttributes(Iterable("REF", "ALT", "DP","GT").asJava)
    .setVcfHeaderFilename("test-data/genomicsdb/template_vcf_header.vcf")
    .setSegmentSize(40)
  
  // interval list 
  val tileInterval = Coordinates.TileDBColumnInterval.newBuilder()
    .setBegin(0)
    .setEnd(1000000000)

  val colIntervalBase = Coordinates.GenomicsDBColumnInterval.newBuilder()
    .setTiledbColumnInterval(tileInterval)

  val colInterval = Coordinates.GenomicsDBColumnOrInterval.newBuilder()
    .setColumnInterval(colIntervalBase)

  val colRanges = GenomicsDBExportConfiguration.GenomicsDBColumnOrIntervalList.newBuilder()
    .addColumnOrIntervalList(colInterval)

  // row ranges
  val rows = GenomicsDBExportConfiguration.RowRange.newBuilder()
    .setLow(0).setHigh(3)

  val rowLists = GenomicsDBExportConfiguration.RowRangeList.newBuilder()
    .addRangeList(rows)

  val builder = baseBuilder.addQueryColumnRanges(colRanges)
    //.addQueryContigIntervals(ci1)
    .addQueryRowRanges(rowLists)
  
  println(builder.build().toString())
  val pb = builder.build().toByteArray();
  val pbQueryFile = Base64.getEncoder().encodeToString(pb);

}

object TestProto2 {

  val baseBuilder = GenomicsDBExportConfiguration.ExportConfiguration.newBuilder()
    .setWorkspace("/nfs3/converge_10640/histogram/workspace")
    .setArrayName("array_0")
    .setReferenceGenome("/nfs/home/blhill/code/github/vagrant-vm/data/human_g1k_v37.fasta")
    .addAllAttributes(Iterable("REF", "ALT", "GT").asJava)
    .setVcfHeaderFilename("test-data/genomicsdb/template_vcf_header.vcf")
    .setSegmentSize(40)
  
  // interval list 
  val tileInterval = Coordinates.TileDBColumnInterval.newBuilder()
    .setBegin(0)
    .setEnd(1000000)

  val colIntervalBase = Coordinates.GenomicsDBColumnInterval.newBuilder()
    .setTiledbColumnInterval(tileInterval)

  val colInterval = Coordinates.GenomicsDBColumnOrInterval.newBuilder()
    .setColumnInterval(colIntervalBase)

  val colRanges = GenomicsDBExportConfiguration.GenomicsDBColumnOrIntervalList.newBuilder()
    .addColumnOrIntervalList(colInterval)

  // row ranges
  val rows = GenomicsDBExportConfiguration.RowRange.newBuilder()
    .setLow(0).setHigh(1000)

  val rowLists = GenomicsDBExportConfiguration.RowRangeList.newBuilder()
    .addRangeList(rows)

  val builder = baseBuilder.addQueryColumnRanges(colRanges)
    //.addQueryContigIntervals(ci1)
    .addQueryRowRanges(rowLists)
  
  println(builder.build().toString())
  val pb = builder.build().toByteArray();
  val pbQueryFile = Base64.getEncoder().encodeToString(pb);

}

class GenomicsDBDataSourceSuite extends GlowBaseTest {

  // todo current setup doesn't support this
  val sourceName = "genomicsdb"
  val longName = "io.projectglow.genomicsdb"
  
  lazy val testLoaderJson = s"$testDataHome/genomicsdb/loader.json"
  lazy val testLoaderJson1 = s"$testDataHome/genomicsdb/loader0.json"
  lazy val testLoaderJson2 = "/nfs3/converge_10640/histogram/jsonfinal/loader_config_file.json"
  lazy val testVcf = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy val multiAllelicVcf = s"$testDataHome/combined.chr20_18210071_18210093.g.vcf"
  lazy val tgpVcf = s"$testDataHome/1000genomes-phase3-1row.vcf"
  lazy val stringInfoFieldsVcf = s"$testDataHome/test.chr17.vcf"
  lazy val sess = spark

  // TODO move all this to a utility

  override def sparkConf: SparkConf = {
    super
      .sparkConf
      .set("spark.hadoop.io.compression.codecs", "org.seqdoop.hadoop_bam.util.BGZFCodec")
  }

  def makeVcfLine(strSeq: Seq[String]): String = {
    (Seq("1", "1", "id", "C", "T,GT", "1", ".") ++ strSeq).mkString("\t")
  }

  test("default schema") {
    val df = spark.read.format(longName)
      .option("genomicsdb.input.loaderjsonfile", testLoaderJson2)
      .option("genomicsdb.input.queryprotobuf", TestProto2.pbQueryFile)
      .load()
    df.show()
    //df.select("genotypes").show(false)  
  
    val variants_per_block = 1000
    val sample_block_count = 10
    val genotypes = df.withColumn("values", mean_substitute(genotype_states(col("genotypes"))))
    genotypes.show()
    //val sample_ids = get_sample_ids(genotypes)
    //block_df, sample_blocks = block_variants_and_samples
  }

  /**test("native genomicsdb"){
    val df = spark.read.format("org.genomicsdb.spark.sources.GenomicsDBSource")
      .option("genomicsdb.input.loaderjsonfile", testLoaderJson2)
      .option("genomicsdb.input.queryprotobuf", TestProto2.pbQueryFile)
      .load()
    df.show()
  }

  test("native genomicsdb query"){
    val df = spark.read.format("org.genomicsdb.spark.sources.GenomicsDBSource")
      .schema(GenomicsDBSchemaFactory.intervalSchema())
      .option("genomicsdb.input.loaderjsonfile", testLoaderJson2)
      .option("genomicsdb.input.queryprotobuf", TestProto2.pbQueryFile)
      .option("genomicsdb.reader.type", "GenomicsDBQuery")
      .load()
    df.show()
  }**/

}
