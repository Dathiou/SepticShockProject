
package edu.gatech.cse8803.ioutils

import org.apache.spark.annotation.{Experimental}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.feature.VectorTransformer
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

/**
  * A feature transformer that projects vectors to a low-dimensional space using PCA.
  *
  * @param k number of principal components
  */

class PCA  ( val k: Int) {
  require(k >= 1, s"PCA requires a number of principal components k >= 1 but was given $k")

  /**
    * Computes a [[PCAModel]] that contains the principal components of the input vectors.
    *
    * @param sources source vectors
    */

  def fit(sources: RDD[Vector]): PCAModel = {
    require(k <= sources.first().size,
      s"source vector size is ${sources.first().size} must be greater than k=$k")

    val mat = new RowMatrix(sources)
    val pc = mat.computePrincipalComponents(k) match {
      case dm: DenseMatrix =>
        dm
      case sm: SparseMatrix =>
        /* Convert a sparse matrix to dense.
         *
         * RowMatrix.computePrincipalComponents always returns a dense matrix.
         * The following code is a safeguard.
         */
        sm.toDense
      case m =>
        throw new IllegalArgumentException("Unsupported matrix format. Expected " +
          s"SparseMatrix or DenseMatrix. Instead got: ${m.getClass}")

    }
    new PCAModel(k, pc)
  }

  /**
    * Java-friendly version of [[fit()]]
    */

  def fit(sources: JavaRDD[Vector]): PCAModel = fit(sources.rdd)
}

/**
  * Model fitted by [[PCA]] that can project vectors to a low-dimensional space using PCA.
  *
  * @param k number of principal components.
  * @param pc a principal components Matrix. Each column is one principal component.
  */

class PCAModel  (
                                 val k: Int,
                                 val pc: DenseMatrix) extends VectorTransformer {
  /**
    * Transform a vector by computed Principal Components.
    *
    * @param vector vector to be transformed.
    *               Vector must be the same length as the source vectors given to [[PCA.fit()]].
    * @return transformed vector. Vector will be of length k.
    */

  override def transform(vector: Vector): Vector = {
    vector match {
      case dv: DenseVector =>
        pc.transpose.multiply(dv)
      case SparseVector(size, indices, values) =>
        /* SparseVector -> single row SparseMatrix */
        val sm = Matrices.sparse(size, 1, Array(0, indices.length), indices, values).transpose
        val projection = sm.multiply(pc)
        Vectors.dense(projection.values)
      case _ =>
        throw new IllegalArgumentException("Unsupported vector format. Expected " +
          s"SparseVector or DenseVector. Instead got: ${vector.getClass}")
    }
  }
}