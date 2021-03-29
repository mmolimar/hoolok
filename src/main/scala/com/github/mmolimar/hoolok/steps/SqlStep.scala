package com.github.mmolimar.hoolok.steps

import com.github.mmolimar.hoolok.HoolokStepConfig
import com.github.mmolimar.hoolok.annotations.StepKind
import com.github.mmolimar.hoolok.common.InvalidStepConfigException
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Calendar
import java.util.concurrent.TimeUnit

@StepKind(kind = "sql")
class SqlStep(config: HoolokStepConfig)
             (implicit spark: SparkSession) extends BaseStep(config)(spark) {

  val query: String = SqlStep.render(config.options.flatMap(_.get("query")).getOrElse {
    throw new InvalidStepConfigException("SQL step is not configured properly. The option 'query' is expected.")
  })

  def processInternal(): DataFrame = {
    logInfo(s"Query to be executed in step: '$query'.")
    spark.sql(query)
  }

}

import com.hubspot.jinjava.lib.fn.ELFunctionDefinition
import com.hubspot.jinjava.{Jinjava, JinjavaConfig}
import org.apache.spark.sql.catalyst.util.{DateTimeConstants => SparkDateTimeConstants}

import java.util.Collections

private[steps] object SqlStep {

  val ns = "hoolok_utils"

  private val jinjava = {
    val jj = new Jinjava(new JinjavaConfig)
    customFunctions.foreach(fn => jj.getGlobalContext.registerFunction(fn))
    jj
  }

  def render(template: String): String = jinjava.render(template, Collections.emptyMap())

  // scalastyle:off
  private def customFunctions: Seq[ELFunctionDefinition] = {
    Seq(
      new ELFunctionDefinition(
        ns, "current_timestamp", classOf[macros], "currentTimestamp"),
      new ELFunctionDefinition(
        ns, "current_year", classOf[macros], "currentYear"),
      new ELFunctionDefinition(
        ns, "current_month", classOf[macros], "currentMonth"),
      new ELFunctionDefinition(
        ns, "current_day", classOf[macros], "currentDay"),
      new ELFunctionDefinition(
        ns, "current_day_of_week", classOf[macros], "currentDayOfWeek"),
      new ELFunctionDefinition(
        ns, "current_day_of_year", classOf[macros], "currentDayOfYear"),
      new ELFunctionDefinition(
        ns, "current_hour", classOf[macros], "currentHour"),
      new ELFunctionDefinition(
        ns, "current_hour_of_day", classOf[macros], "currentHourOfDay"),
      new ELFunctionDefinition(
        ns, "current_minute", classOf[macros], "currentMinute"),

      new ELFunctionDefinition(
        ns, "previous_year", classOf[macros], "previousYear"),
      new ELFunctionDefinition(
        ns, "previous_month", classOf[macros], "previousMonth"),
      new ELFunctionDefinition(
        ns, "previous_day", classOf[macros], "previousDay"),
      new ELFunctionDefinition(
        ns, "previous_day_of_week", classOf[macros], "previousDayOfWeek"),
      new ELFunctionDefinition(
        ns, "previous_day_of_year", classOf[macros], "previousDayOfYear"),
      new ELFunctionDefinition(
        ns, "previous_hour", classOf[macros], "previousHour"),
      new ELFunctionDefinition(
        ns, "previous_hour_of_day", classOf[macros], "previousHourOfDay"),
      new ELFunctionDefinition(
        ns, "previous_minute", classOf[macros], "previousMinute"),

      new ELFunctionDefinition(ns, "date_add_days", classOf[macros],
        "dateAddDays", classOf[String], classOf[String], classOf[String], classOf[Int]),
      new ELFunctionDefinition(ns, "date_add_weeks", classOf[macros],
        "dateAddWeeks", classOf[String], classOf[String], classOf[String], classOf[Int]),
      new ELFunctionDefinition(ns, "date_add_months", classOf[macros],
        "dateAddMonths", classOf[String], classOf[String], classOf[String], classOf[Int]),
      new ELFunctionDefinition(ns, "date_add_years", classOf[macros],
        "dateAddYears", classOf[String], classOf[String], classOf[String], classOf[Int]),
      new ELFunctionDefinition(ns, "last_day_current_month", classOf[macros],
        "lastDayOfCurrentMonth"),
      new ELFunctionDefinition(ns, "last_day_of_month", classOf[macros],
        "lastDayOfMonth", classOf[String], classOf[String]),
      new ELFunctionDefinition(ns, "to_lower", classOf[macros],
        "toLower", classOf[String]),
      new ELFunctionDefinition(ns, "to_upper", classOf[macros],
        "toUpper", classOf[String]),
      new ELFunctionDefinition(ns, "capitalize", classOf[macros],
        "capitalize", classOf[String])
    )
  }
}

// scalastyle:off
private[steps] class macros

private[steps] object macros {

  def currentTimestamp(): Long = {
    val instant = Instant.now
    val us = Math.multiplyExact(instant.getEpochSecond, SparkDateTimeConstants.MICROS_PER_SECOND)
    val result = Math.addExact(us, TimeUnit.NANOSECONDS.toMicros(instant.getNano))
    result
  }

  def currentYear(): Int = LocalDateTime.now.getYear

  def currentMonth(): Int = LocalDateTime.now.getMonthValue

  def currentDay(): Int = LocalDateTime.now.getDayOfMonth

  def currentDayOfWeek(): Int = LocalDateTime.now.getDayOfWeek.getValue

  def currentDayOfYear(): Int = LocalDateTime.now.getDayOfYear

  def currentHour(): Int = LocalDateTime.now.getHour

  def currentHourOfDay(): Int = LocalDateTime.now.getHour % 12

  def currentMinute(): Int = LocalDateTime.now.getMinute

  def previousYear(): Int = LocalDateTime.now.minus(Period.ofYears(1)).getYear

  def previousMonth(): Int = LocalDateTime.now.minus(Period.ofMonths(1)).getMonthValue

  def previousDay(): Int = LocalDateTime.now.minus(Period.ofDays(1)).getDayOfMonth

  def previousDayOfWeek(): Int = LocalDateTime.now.minus(Period.ofDays(1)).getDayOfWeek.getValue

  def previousDayOfYear(): Int = LocalDateTime.now.minus(Period.ofDays(1)).getDayOfYear

  def previousHour(): Int = LocalDateTime.now.minus(Duration.of(1, ChronoUnit.HOURS)).getHour

  def previousHourOfDay(): Int = LocalDateTime.now.minus(Duration.of(1, ChronoUnit.HOURS)).getHour % 12

  def previousMinute(): Int = LocalDateTime.now.minus(Duration.of(1, ChronoUnit.MINUTES)).getMinute

  def dateAddDays(date: String, inputDateFormat: String, outputDateFormat: String, numDays: Int): String = {
    LocalDate.parse(date, DateTimeFormatter.ofPattern(inputDateFormat)).plusDays(numDays)
      .format(DateTimeFormatter.ofPattern(outputDateFormat))
  }

  def dateAddWeeks(date: String, inputDateFormat: String, outputDateFormat: String, numWeeks: Int): String = {
    LocalDate.parse(date, DateTimeFormatter.ofPattern(inputDateFormat)).plusWeeks(numWeeks)
      .format(DateTimeFormatter.ofPattern(outputDateFormat))
  }

  def dateAddMonths(date: String, inputDateFormat: String, outputDateFormat: String, numMonths: Int): String = {
    LocalDate.parse(date, DateTimeFormatter.ofPattern(inputDateFormat)).plusMonths(numMonths)
      .format(DateTimeFormatter.ofPattern(outputDateFormat))
  }

  def dateAddYears(date: String, inputDateFormat: String, outputDateFormat: String, numYears: Int): String = {
    LocalDate.parse(date, DateTimeFormatter.ofPattern(inputDateFormat)).plusYears(numYears)
      .format(DateTimeFormatter.ofPattern(outputDateFormat))
  }

  def lastDayOfCurrentMonth: Int = Calendar.getInstance.getActualMaximum(Calendar.DAY_OF_MONTH)

  def lastDayOfMonth(date: String, dateFormat: String): Int = {
    LocalDate.parse(date, DateTimeFormatter.ofPattern(dateFormat)).lengthOfMonth()
  }

  def toLower(str: String): String = str.toLowerCase

  def toUpper(str: String): String = str.toUpperCase

  def capitalize(str: String): String = str.capitalize

}
