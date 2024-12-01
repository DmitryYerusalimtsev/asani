package com.dyeru.asani.arrow

trait Processor[In, Out] {
  def process(in: List[In]): List[Out]
}
