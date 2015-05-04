package com.twitter.lsh.stores

/**
 * Identifes which an individual lsh table.
 * @param tableId Id of the table
 * @param numHashes Number of hashes in teh table
 * @param familyHash Family for this table.
 */
case class TableIdentifier(tableId: Int, numHashes: Int, familyHash: Int) extends Ordered[TableIdentifier] {
  import scala.math.Ordered.orderingToOrdered
  def compare(that: TableIdentifier): Int = {
    (this.tableId, this.numHashes, this.familyHash) compare (that.tableId, that.numHashes, that.familyHash)
  }
}

