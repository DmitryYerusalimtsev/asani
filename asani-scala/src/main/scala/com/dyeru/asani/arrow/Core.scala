package com.dyeru.asani.arrow

trait Processor[In, Out] {
  def process[In, Out](in: List[In]): List[Out]
}

trait Proce