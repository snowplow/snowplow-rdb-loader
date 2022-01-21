package com.snowplowanalytics.snowplow.rdbloader.db.helpers

import doobie.util.fragment.Fragment

trait FragmentEncoder[T] {
  def encode(t: T): Fragment
}
