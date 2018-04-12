package org.sparktesting.domain

import java.sql.Timestamp

case class PageView(timestamp: Timestamp, site: String, requests: Int)