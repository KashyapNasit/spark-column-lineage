package com.nasit

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, BinaryExpression, Concat, Expression, Literal, NamedExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.mutable

class QueryListener extends QueryExecutionListener {

  def getRootColumns(expr: Expression): mutable.Set[String] = {
    val inputColumns = mutable.Set[String]()

    expr match {
      case attr: AttributeReference =>
        inputColumns += attr.toString()

      case alias: Alias =>
        inputColumns += alias.toString()
        inputColumns ++= getRootColumns(alias.child)

      case concat: Concat =>
        concat.children.map(getRootColumns).foreach(e => inputColumns ++= e)

      case aggregate: AggregateExpression =>
        aggregate.children.map(getRootColumns).foreach(e => inputColumns ++= e)

      case avg: Average =>
        inputColumns ++= getRootColumns(avg.child)

      case unaryExpr: UnaryExpression =>
        inputColumns ++= getRootColumns(unaryExpr.child)

      case binaryExpr: BinaryExpression =>
        inputColumns ++= getRootColumns(binaryExpr.left)
        inputColumns ++= getRootColumns(binaryExpr.right)

      case _ => println(expr.getClass)
    }

    inputColumns
  }

  def traversePlan(plan: LogicalPlan): mutable.Map[String, mutable.Set[String]] = {
    val columnLineage = mutable.Map[String, mutable.Set[String]]()

    plan match {
      case GlobalLimit(limitExpr, child) =>
        columnLineage ++= traversePlan(child)

      case LocalLimit(limitExpr, child) =>
        columnLineage ++= traversePlan(child)

      case Aggregate(groupingExpressions, aggregateExpressions, child) =>
        aggregateExpressions.map(getInputColumns).foreach(e => columnLineage ++= e)
        columnLineage ++= traversePlan(child)

      case Project(projectList, child) =>
        projectList.map(getInputColumns).foreach(e => columnLineage ++= e)
        columnLineage ++= traversePlan(child)

      case unaryNode: UnaryNode =>
        columnLineage ++= traversePlan(unaryNode.child)

      case binaryNode: BinaryNode =>
        columnLineage ++= traversePlan(binaryNode.left)
        columnLineage ++= traversePlan(binaryNode.right)

      case logicalRelation: LogicalRelation =>
        val inputColumns = logicalRelation.output.map(_.toString()).to[mutable.Set]
        columnLineage ++= inputColumns.map(c => (c,mutable.Set("SOURCE")))

      case e => println(e)
    }

    columnLineage
  }

  def getInputColumns(expr: Expression): mutable.Map[String, mutable.Set[String]] = {
    val inputColumns = mutable.Set[String]()
    val columnLineage = mutable.Map[String, mutable.Set[String]]()

    expr match {
      case attr: AttributeReference =>
        columnLineage += attr.toString() -> mutable.Set("SOURCE")

      case alias: Alias =>
        inputColumns ++= getRootColumns(alias.child)
        columnLineage += alias.name + "#" + alias.exprId.id -> inputColumns

      case concat: Concat =>
        concat.children.map(getRootColumns).foreach(e => inputColumns ++= e)
        columnLineage += concat.toString() -> inputColumns

      case aggregate: AggregateExpression =>
        aggregate.children.map(getRootColumns).foreach(e => inputColumns ++= e)
        columnLineage += aggregate.toString() -> inputColumns

      case avg: Average =>
        inputColumns ++= getRootColumns(avg.child)
        columnLineage += avg.toString() -> inputColumns

      case unaryExpr: UnaryExpression =>
        inputColumns ++= getRootColumns(unaryExpr.child)
        columnLineage += unaryExpr.toString() -> inputColumns

      case binaryExpr: BinaryExpression =>
        inputColumns ++= getRootColumns(binaryExpr.left)
        inputColumns ++= getRootColumns(binaryExpr.right)
        columnLineage += binaryExpr.toString() -> inputColumns

      case _ => println(expr.getClass)
    }

    columnLineage
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    println("=========================================")
    println("                 Print on success                 ")
    println(s"${funcName}: ${qe.logical}")

    val logicalPlan = qe.logical

    val columnLineage = traversePlan(logicalPlan)

    for ((outputColumn, inputColumns) <- columnLineage.toList.sortBy(_._2.toString())) {
      println(s"Output column: $outputColumn")
      println(s"Input columns: ${inputColumns.mkString(", ")}")
    }

    println("=========================================")
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {

  }
}
