package io.projectglow.genomicsdb

import scala.collection.JavaConverters._

import htsjdk.samtools.ValidationStringency
import htsjdk.tribble.{CloseableTribbleIterator, FeatureCodec}
import htsjdk.tribble.readers.PositionalBufferedStream
import htsjdk.variant.bcf2.BCF2Codec
import htsjdk.variant.variantcontext.VariantContext

import org.genomicsdb.model.GenomicsDBExportConfiguration
import org.genomicsdb.reader.GenomicsDBFeatureReader
import org.genomicsdb.spark.{GenomicsDBConfiguration, GenomicsDBInput, GenomicsDBVidSchema, GenomicsDBSchemaFactory}
import org.genomicsdb.spark.sources.{GenomicsDBBatch, GenomicsDBInputPartition}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.read.{Batch, Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.projectglow.vcf.VariantContextToInternalRowConverter

//class GenomicsDBDataSource extends TableProvider {
class DefaultSource extends TableProvider {
  
  def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // TODO make this read from the glow VCFSchema
    return GenomicsDBSchemaFactory.glowCompatSchema()
  }  

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: java.util.Map[String, String]): Table = {
    new GenomicsDBTable(schema, partitioning, properties)
  }

  override def supportsExternalMetadata(): Boolean = false

}

class GenomicsDBTable(schema: StructType, partitioning: Array[Transform], properties: java.util.Map[String,String]) extends Table with SupportsRead {

  override def capabilities(): java.util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def schema(): StructType = schema

  override def name(): String = this.getClass.toString

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new GenomicsDBScanBuilder(schema, properties, options)

}

class GenomicsDBScanBuilder(schema: StructType, properties: java.util.Map[String,String], 
  options: CaseInsensitiveStringMap) extends ScanBuilder {
  
    override def build(): Scan = new GenomicsDBScan(schema, properties, options)

}

class GenomicsDBScan(schema: StructType, properties: java.util.Map[String,String], 
  options: CaseInsensitiveStringMap) extends Scan with Batch {

    private val batch: GenomicsDBBatch = new GenomicsDBBatch(schema, properties, options)
    
    private val input:GenomicsDBInput[GenomicsDBInputPartition] = batch.getInput()
  
    override def readSchema(): StructType = schema
  
    override def toBatch(): Batch = this

    override def planInputPartitions(): Array[InputPartition] = batch.planInputPartitions()

    override def createReaderFactory(): PartitionReaderFactory = new GenomicsDBPartitionReaderFactory()

}

class GenomicsDBPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = 
    new GenomicsDBPartitionReader(partition.asInstanceOf[GenomicsDBInputPartition])
}

class GenomicsDBPartitionReader(inputPartition: GenomicsDBInputPartition) extends PartitionReader[InternalRow]{
 
    val exportConfiguration: GenomicsDBExportConfiguration.ExportConfiguration =
      try {
          GenomicsDBInput.createTargetExportConfigurationPB(
            inputPartition.getQuery(), inputPartition.getPartitionInfo(),
            inputPartition.getQueryInfoList(), inputPartition.getQueryIsPB())
      } catch {
        case _:java.text.ParseException | _:java.io.IOException => null
      } 
    
    val bcfCode = new BCF2Codec()
    val fReader = 
      try {
          new GenomicsDBFeatureReader(
              exportConfiguration,
              bcfCode.asInstanceOf[FeatureCodec[VariantContext, PositionalBufferedStream]],
              java.util.Optional.of(inputPartition.getLoader()))
      } catch {
        case _:java.io.IOException => null;
      }
    
    // converter components
    val header = fReader.getVcfHeader()
    val requiredSchema = GenomicsDBSchemaFactory.glowCompatSchema()
    val stringency = ValidationStringency.LENIENT
    val converter = new VariantContextToInternalRowConverter(header, requiredSchema, stringency)

    // build internal row iterator
    val fReaderIterator = fReader.iterator
    val iterator = fReaderIterator.iterator.asScala

    def next(): Boolean = iterator.hasNext
    
    def get(): InternalRow = converter.convertRow(iterator.next(), false)

    def close(): Unit = fReaderIterator.close()

}  
