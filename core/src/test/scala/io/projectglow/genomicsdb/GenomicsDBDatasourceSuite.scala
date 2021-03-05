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

import java.nio.file.Files
import java.util.Base64

import htsjdk.variant.vcf.VCFConstants
import org.genomicsdb.model.GenomicsDBExportConfiguration
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

class GenomicsDBDataSourceSuite extends GlowBaseTest {

  // todo current setup doesn't support this
  val sourceName = "genomicsdb"
  val longName = "io.projectglow.genomicsdb"
  
  lazy val testLoaderJson = s"$testDataHome/genomicsdb/loader.json"
  lazy val testQueryJson = s"$testDataHome/genomicsdb/query.json"
  lazy val testVcf = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy val multiAllelicVcf = s"$testDataHome/combined.chr20_18210071_18210093.g.vcf"
  lazy val tgpVcf = s"$testDataHome/1000genomes-phase3-1row.vcf"
  lazy val stringInfoFieldsVcf = s"$testDataHome/test.chr17.vcf"
  lazy val sess = spark

  val builder = GenomicsDBExportConfiguration.ExportConfiguration.newBuilder();
  val jsonString = scala.io.Source.fromFile(testQueryJson).mkString
  JsonFormat.merge(jsonString, builder);
  val pb = builder.build().toByteArray();
  val pbQueryFile = Base64.getEncoder().encodeToString(pb);
  println(pbQueryFile)

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
      .option("genomicsdb.input.loaderjsonfile", testLoaderJson)
      .option("genomicsdb.input.queryprotobuf", pbQueryFile)
      .load()
    df.show(false)
    //assert(df.schema.exists(_.name.startsWith("INFO_")))
    //assert(df.where(expr("size(filter(genotypes, g -> g.sampleId is null)) > 0")).count() == 0)
  }

  /*test("parse VCF") {
    val datasource = spark.read.format(sourceName).load(testVcf)
    datasource.count()
  }

  test("no sample ids") {
    val schema = spark
      .read
      .format(sourceName)
      .option("includeSampleIds", false)
      .load(testVcf)
      .withColumn("g", expr("genotypes[0]"))
      .selectExpr("g.*")
      .schema

    assert(!schema.exists(_.name == "sampleId"))
  }

  test("with sample ids") {
    val datasource = spark
      .read
      .format(sourceName)
      .load(testVcf)
    val size = datasource.count()
    assert(datasource.where("genotypes[0].sampleId = 'NA12878'").count() == size)
  }**/
}
