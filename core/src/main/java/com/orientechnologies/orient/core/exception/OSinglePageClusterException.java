package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.impl.local.paginated.OSinglePageCluster;

public class OSinglePageClusterException extends ODurableComponentException {
  public OSinglePageClusterException(OSinglePageClusterException exception) {
    super(exception);
  }

  public OSinglePageClusterException(String message, OSinglePageCluster component) {
    super(message, component);
  }
}
