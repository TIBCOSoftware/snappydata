package io.snappydata.tools

import java.io.{File, IOException}

import com.gemstone.gemfire.internal.{GemFireTerminateError, GemFireUtilLauncher}
import com.pivotal.gemfirexd.internal.iapi.tools.i18n.LocalizedResource
import com.pivotal.gemfirexd.tools.GfxdUtilLauncher
import io.snappydata.LocalizedMessages

/**
 * Launcher class encompassing snappy processes command lines.
 *
 * Created by soubhikc on 30/09/15.
 */
class SnappyUtilLauncher extends GfxdUtilLauncher {

  protected override def getTypes() : java.util.Map[String, GemFireUtilLauncher#CommandEntry] = {
    val types = super.getTypes()

    types.put("leader", new CommandEntry(classOf[LeaderLauncher],
      LocalizedMessages.res.getTextMessage("UTIL_Leader_Usage"), false))

    types
  }

  override def invoke(args: Array[String]) = {
    super.invoke(args)
  }

  override def validateArgs (args: Array[String]) = {
    super.validateArgs(args)
  }
}


object SnappyUtilLauncher {

  private val SCRIPT_NAME: String = "snappy"
  private val GET_CANONICAL_PATH_ARG: String = "--get-canonical-path"

  /**
   * @see GemFireUtilLauncher#main(String[])
   **/
  def main(args: Array[String]) : Unit = {

    val launcher = new SnappyUtilLauncher()

    try {
      // no args will default to using ij
      if (args.length == 0) {
        launcher.invoke(Array(SCRIPT_NAME))
      }
      // short-circuit for the internal "--get-canonical-path" argument used by
      // script to resolve the full path including symlinks (#43722)
      else if (args.length == 2 && GET_CANONICAL_PATH_ARG.equals(args(0))) {
        try {
          System.out.println(new File(args(1)).getCanonicalPath())
        } catch {
          case ioe: IOException =>
              // in case of any exception print the given path itself
          System.out.println(args(1))
        }

        return;

      } else {
        launcher.validateArgs(args)
        launcher.invoke(args)
      }
    } catch {
      case term: GemFireTerminateError => System.exit(term.getExitCode())
      case re: RuntimeException =>
        // look for a GemFireTerminateError inside
        var cause = re.getCause()
        while (cause != null) {
          if (cause.isInstanceOf[GemFireTerminateError]) {
            System.exit(cause.asInstanceOf[GemFireTerminateError].getExitCode())
          }
          cause = cause.getCause()
        }
        throw re;

    }

  }

}