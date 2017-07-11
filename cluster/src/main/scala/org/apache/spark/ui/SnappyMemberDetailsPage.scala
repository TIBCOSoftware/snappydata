package org.apache.spark.ui

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable
import scala.util.control.Breaks._
import scala.xml.{Unparsed, Node}

import io.snappydata.SnappyTableStatsProviderService

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils
import org.apache.spark.util.logging.RollingFileAppender


private[ui] class SnappyMemberDetailsPage (parent: SnappyDashboardTab)
    extends WebUIPage("memberDetails") with Logging {

  private var workDir: File = null
  private var logFileName: String = null
  private val defaultBytes = 100 * 1024

  private def createPageTitleNode(title: String): Seq[Node] = {

    val sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss")
    val lastUpdatedOn = sdf.format(new Date())

    <div class="row-fluid">
      <div class="span12">
        <h3 style="vertical-align: bottom; display: inline-block;">
          {title}
        </h3>
        <span style="float:right; font-size: 12px;" data-toggle="tooltip" title=""
              data-original-title="Reload page to refresh Dashboard." >Last updated on {
          lastUpdatedOn
          }</span>
      </div>
    </div>
  }

  private def getMemberStats(memberDetails: mutable.Map[String, Any]): Seq[Node] = {

    val status = memberDetails.getOrElse("status", "")

    val statusImgUri = if(status.toString.equalsIgnoreCase("running")) {
      "/static/snappydata/running-status-icon-70x68.png"
    } else {
      "/static/snappydata/warning-status-icon-70x68.png"
    }

    val cpuUsage = memberDetails.getOrElse("cpuActive",0).asInstanceOf[Integer].toDouble;

    val heapStoragePoolUsed = memberDetails.getOrElse("heapStoragePoolUsed", 0).asInstanceOf[Long]
    val heapStoragePoolSize = memberDetails.getOrElse("heapStoragePoolSize", 0).asInstanceOf[Long]
    val heapExecutionPoolUsed = memberDetails.getOrElse("heapExecutionPoolUsed", 0).asInstanceOf[Long]
    val heapExecutionPoolSize = memberDetails.getOrElse("heapExecutionPoolSize", 0).asInstanceOf[Long]

    val offHeapStoragePoolUsed = memberDetails.getOrElse("offHeapStoragePoolUsed", 0).asInstanceOf[Long]
    val offHeapStoragePoolSize = memberDetails.getOrElse("offHeapStoragePoolSize", 0).asInstanceOf[Long]
    val offHeapExecutionPoolUsed = memberDetails.getOrElse("offHeapExecutionPoolUsed", 0).asInstanceOf[Long]
    val offHeapExecutionPoolSize = memberDetails.getOrElse("offHeapExecutionPoolSize", 0).asInstanceOf[Long]

    val heapMemorySize = memberDetails.getOrElse("heapMemorySize", 0).asInstanceOf[Long]
    val heapMemoryUsed = memberDetails.getOrElse("heapMemoryUsed", 0).asInstanceOf[Long]
    val offHeapMemorySize = memberDetails.getOrElse("offHeapMemorySize", 0).asInstanceOf[Long]
    val offHeapMemoryUsed = memberDetails.getOrElse("offHeapMemoryUsed", 0).asInstanceOf[Long]
    val jvmHeapSize = memberDetails.getOrElse("totalMemory", 0).asInstanceOf[Long]
    val jvmHeapUsed = memberDetails.getOrElse("usedMemory",0).asInstanceOf[Long]

    var memoryUsage:Long = 0
    if((heapMemorySize + offHeapMemorySize) > 0) {
      memoryUsage = (heapMemoryUsed + offHeapMemoryUsed) * 100 /
          (heapMemorySize + offHeapMemorySize)
    }
    var jvmHeapUsage:Long = 0
    if(jvmHeapSize > 0) {
      jvmHeapUsage = jvmHeapUsed * 100 / jvmHeapSize
    }

    <div class="row-fluid">
      <div class="keyStates" style="width: 300px;">
        <div class="keyStatesText" style="text-align: left;">
          Member : <span>{memberDetails.getOrElse("id","NA")}</span>
        </div>
        <div class="keyStatesText" style="text-align: left;">
          Status : <span>{status}</span>
        </div>
        <div class="keyStatesText" style="text-align: left;">
          processId : <span>{memberDetails.getOrElse("processId","").toString}</span>
        </div>
      </div>
      <div class="keyStates">
        <div class="keyStatsValue"
             style="width:50%; margin: auto;" data-toggle="tooltip" title=""
             data-original-title={
             SnappyMemberDetailsPage.memberStats("status").toString + ": " + status.toString
             } >
          <img style="padding-top: 15px;" src={statusImgUri} />
        </div>
        <div class="keyStatesText">{SnappyMemberDetailsPage.memberStats("status")}</div>
      </div>
      <div class="keyStates">
        <div class="keyStatsValue" id="cpuUsage" data-value={cpuUsage.toString}
             data-toggle="tooltip" title=""
             data-original-title={
             SnappyMemberDetailsPage.memberStats("cpuUsageTooltip").toString
             }>
          <svg id="cpuUsageGauge" width="100%" height="100%" ></svg>
        </div>
        <div class="keyStatesText">{SnappyMemberDetailsPage.memberStats("cpuUsage")}</div>
      </div>
      <div class="keyStates">
        <div class="keyStatsValue" id="memoryUsage" data-value={memoryUsage.toString}
             data-toggle="tooltip" title=""
             data-original-title={
             SnappyMemberDetailsPage.memberStats("memoryUsageTooltip").toString
             }>
          <svg id="memoryUsageGauge" width="100%" height="100%" ></svg>
        </div>
        <div class="keyStatesText">{SnappyMemberDetailsPage.memberStats("memoryUsage")}</div>
      </div>
      <div class="keyStates">
        <div class="keyStatsValue" id="jvmHeapUsage" data-value={jvmHeapUsage.toString}
             data-toggle="tooltip" title=""
             data-original-title={
             SnappyMemberDetailsPage.memberStats("jvmHeapUsageTooltip").toString
             }>
          <svg id="jvmHeapUsageGauge" width="100%" height="100%" ></svg>
        </div>
        <div class="keyStatesText">{SnappyMemberDetailsPage.memberStats("jvmHeapUsage")}</div>
      </div>
    </div>
  }

  override def render(request: HttpServletRequest): Seq[Node] = {

    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt).getOrElse(defaultBytes)

    val memberId = Option(request.getParameter("memId")).map { memberId =>
      UIUtils.decodeURLParameter(memberId)
    }.getOrElse {
      throw new IllegalArgumentException(s"Missing memId parameter")
    }

    val allMembers = SnappyTableStatsProviderService.getService.getMembersStatsOnDemand
    val memberDetails: scala.collection.mutable.Map[String, Any] = {
      var mem = scala.collection.mutable.Map.empty[String, Any]
      breakable {
        allMembers.foreach(m => {
          if (m._2("id").toString.equalsIgnoreCase(memberId)) {
            mem = m._2
            break
          }
        })
      }
      mem
    }

    if(memberDetails.isEmpty){
      throw new IllegalArgumentException(s"Missing memId parameter")
    }

    val memberStats = getMemberStats(memberDetails)

    // set members workDir and LogFileName
    workDir = new File(memberDetails.getOrElse("userDir", "").toString)
    logFileName = memberDetails.getOrElse("logFile", "").toString

    val pageHeaderText : String  = SnappyMemberDetailsPage.pageHeaderText

    // Generate Pages HTML
    val pageTitleNode = createPageTitleNode(pageHeaderText)

    var PageContent: Seq[Node] = mutable.Seq.empty

    val memberLogTitle =
      <div class="row-fluid">
        <div class="span12">
          <h4 style="vertical-align: bottom; display: inline-block;"
              data-toggle="tooltip" data-placement="top" title=""
              data-original-title="Member Logs">
            Member Logs
          </h4>
          <div>
            <span style="font-weight: bolder;">Location :</span>
            {memberDetails.getOrElse("userDir", "")}/{memberDetails.getOrElse("logFile", "")}
          </div>
        </div>
      </div>

    // Get logs
    val (logText, startByte, endByte, logLength) =
      getLog(workDir.getPath, logFileName, offset, byteLength)

    val curLogLength = endByte - startByte

    val range =
      <span id="log-data" style="font-weight:bold;">
        Showing {curLogLength} Bytes: {startByte.toString} - {endByte.toString} of {logLength}
      </span>

    val moreButton =
      <button type="button" onclick={"loadMore()"} class="log-more-btn btn btn-default">
        Load More
      </button>

    val newButton =
      <button type="button" onclick={"loadNew()"} class="log-new-btn btn btn-default">
        Load New
      </button>

    val alert =
      <div class="no-new-alert alert alert-info" style="display: none;">
        End of Log
      </div>

    val logParams = "/?memId=%s".format(memberId)

    val jsOnload = "window.onload = " +
        s"initLogPage('$logParams', $curLogLength, $startByte, $endByte, $logLength, $byteLength);"

    val content =
      <div style="margin-top:5px;">
        {range}
        <div class="log-content"
             style="height:60vh; overflow:auto; margin-top:5px; border: 1px solid #E2E2E2;">
          <div>{moreButton}</div>
          <pre>{logText}</pre>
          {alert}
          <div>{newButton}</div>
        </div>
        <script>{Unparsed(jsOnload)}</script>
      </div>

    PageContent = pageTitleNode ++ memberStats ++ memberLogTitle ++ content

    UIUtils.simpleSparkPageWithTabs_2("Member Details Page", PageContent, parent, Some(500))
  }

  def renderLog(request: HttpServletRequest): String = {

    val memberId = Option(request.getParameter("memId")).map { memberId =>
      UIUtils.decodeURLParameter(memberId)
    }.getOrElse {
      throw new IllegalArgumentException(s"Missing memId parameter")
    }

    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt).getOrElse(defaultBytes)

    val (logText, startByte, endByte, logLength) =
      getLog(workDir.getPath, logFileName, offset, byteLength)

    val pre = s"==== Bytes $startByte-$endByte of $logLength of ${workDir.getPath}/$logFileName ====\n"

    pre + logText

  }

  /** Get the part of the log files given the offset and desired length of bytes */
  private def getLog(
      logDirectory: String,
      logFile: String,
      offsetOption: Option[Long],
      byteLength: Int
      ): (String, Long, Long, Long) = {

    if (logFile == null || logFile.isEmpty) {
      return ("Error: Log file must be specified ", 0, 0, 0)
    }

    // Verify that the normalized path of the log directory is in the working directory
    val normalizedUri = new File(logDirectory).toURI.normalize()
    val normalizedLogDir = new File(normalizedUri.getPath)
    if (!Utils.isInDirectory(workDir, normalizedLogDir)) {
      return ("Error: invalid log directory " + logDirectory, 0, 0, 0)
    }

    try {
      val files = RollingFileAppender.getSortedRolledOverFiles(logDirectory, logFile)
      logDebug(s"Sorted log files of type $logFile in $logDirectory:\n${files.mkString("\n")}")

      val fileLengths: Seq[Long] = files.map(Utils.getFileLength(_, parent.parent.conf))
      val totalLength = fileLengths.sum
      val offset = offsetOption.getOrElse(totalLength - byteLength)
      val startIndex = {
        if (offset < 0) {
          0L
        } else if (offset > totalLength) {
          totalLength
        } else {
          offset
        }
      }
      val endIndex = math.min(startIndex + byteLength, totalLength)
      logDebug(s"Getting log from $startIndex to $endIndex")
      val logText = Utils.offsetBytes(files, fileLengths, startIndex, endIndex)
      logDebug(s"Got log of length ${logText.length} bytes")
      (logText, startIndex, endIndex, totalLength)
    } catch {
      case e: Exception =>
        logError(s"Error getting $logFile logs from directory $logDirectory", e)
        ("Error getting logs due to exception: " + e.getMessage, 0, 0, 0)
    }
  }

}

object SnappyMemberDetailsPage{
  val pageHeaderText = "SnappyData Member Details"

  object Status {
    val stopped = "Stopped"
    val running = "Running"
  }

  val ValueNotApplicable = "N/A"

  val memberStats = scala.collection.mutable.HashMap.empty[String, String]
  memberStats += ("status" -> "Status")
  memberStats += ("statusTooltip" -> "Members Status")
  memberStats += ("id" -> "Id")
  memberStats += ("idTooltip" -> "Members unique Identifier")
  memberStats += ("name" -> "Name")
  memberStats += ("nameTooltip" -> "Members Name")
  memberStats += ("nameOrId" -> "Member")
  memberStats += ("nameOrIdTooltip" -> "Members Name/Id")
  memberStats += ("description" -> "Member")
  memberStats += ("descriptionTooltip" -> "Members Description")
  memberStats += ("host" -> "Host")
  memberStats += ("hostTooltip" -> "Physical machine on which member is running")
  memberStats += ("cpuUsage" -> "CPU Usage")
  memberStats += ("cpuUsageTooltip" -> "CPU used by Member Host")
  memberStats += ("memoryUsage" -> "Memory Usage")
  memberStats += ("memoryUsageTooltip" -> "Memory(Heap + Off-Heap) used by Member")
  memberStats += ("usedMemory" -> "Used Memory")
  memberStats += ("usedMemoryTooltip" -> "Used Memory")
  memberStats += ("totalMemory" -> "Total Memory")
  memberStats += ("totalMemoryTooltip" -> "Total Memory")
  memberStats += ("clients" -> "Connections")
  memberStats += ("clientsTooltip" -> "Number of JDBC connections to Member")
  memberStats += ("memberType" -> "Type")
  memberStats += ("memberTypeTooltip" -> "Member is Lead / Locator / Data Server")
  memberStats += ("lead" -> "Lead")
  memberStats += ("leadTooltip" -> "Member is Lead")
  memberStats += ("locator" -> "Locator")
  memberStats += ("locatorTooltip" -> "Member is Locator")
  memberStats += ("server" -> "Server")
  memberStats += ("serverTooltip" -> "Member is Server")
  memberStats += ("storageMemoryUsed" -> "StorageUsed")
  memberStats += ("storageMemoryToolTip" -> "Total storage pool memory used")
  memberStats += ("storageMemoryPoolSize" -> "StoragePoolSize")
  memberStats += ("storageMemorySizeToolTip" -> "Max storage pool memory size")
  memberStats += ("executionMemoryUsed" -> "ExecutionUsed")
  memberStats += ("executionMemoryToolTip" -> "Total execution pool memory used")
  memberStats += ("executionMemoryPoolSize" -> "ExecutionPoolSize")
  memberStats += ("executionMemorySizeToolTip" -> "Max execution pool memory size")
  memberStats += ("heapMemory" -> "Heap Memory (Used / Total)")
  memberStats += ("heapMemoryTooltip" -> "Members used and total Heap Memory")
  memberStats += ("offHeapMemory" -> "Off-Heap Memory (Used / Total)")
  memberStats += ("offHeapMemoryTooltip" -> "Members used and total Off Heap Memory")
  memberStats += ("jvmHeapMemory" -> "JVM Heap (Used / Total)")
  memberStats += ("jvmHeapMemoryTooltip" -> "Members used and total JVM Heap")
  memberStats += ("jvmHeapUsage" -> "JVM Heap Usage")
  memberStats += ("jvmHeapUsageTooltip" -> "Clusters Total JVM Heap Usage")
}