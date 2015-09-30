/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.gopivotal.com/patents.
 *========================================================================
 */

package hydra;

import com.gemstone.gemfire.internal.LogWriterImpl;

/**
 * Provides version-dependent support for logging changes.
 */
public class LogVersionHelper {

  protected static String levelToString(int level) {
    return LogWriterImpl.levelToString(level);
  }

  protected static int levelNameToCode(String level) {
    return LogWriterImpl.levelNameToCode(level);
  }
}
