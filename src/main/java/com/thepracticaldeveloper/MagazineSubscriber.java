package com.thepracticaldeveloper;

import java.util.concurrent.Flow;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MagazineSubscriber implements Flow.Subscriber<Integer> {

  public static final String JACK = "Jack";
  public static final String PETE = "Pete";

  private static final Logger log = LoggerFactory.
    getLogger(MagazineSubscriber.class);

  private final long sleepTime;
  private final String subscriberName;
  private Flow.Subscription subscription;
  private int nextMagazineExpected;
  private int totalRead;

  MagazineSubscriber(final long sleepTime, final String subscriberName) {
    this.sleepTime = sleepTime;
    this.subscriberName = subscriberName;
    this.nextMagazineExpected = 1;
    this.totalRead = 0;
  }

  @Override
  public void onSubscribe(final Flow.Subscription subscription) {
    this.subscription = subscription;
    subscription.request(1);
  }

  @Override
  public void onNext(final Integer magazineNumber) {
    if (magazineNumber != nextMagazineExpected) {
      IntStream.range(nextMagazineExpected, magazineNumber).forEach(
        (msgNumber) ->
          log("Oh no! I missed the magazine " + msgNumber)
      );
      // Catch up with the number to keep tracking missing ones
      nextMagazineExpected = magazineNumber;
    }
    log("Great! I got a new magazine: " + magazineNumber);
    takeSomeRest();
    nextMagazineExpected++;
    totalRead++;

    log("I'll get another magazine now, next one should be: " +
      nextMagazineExpected);
    subscription.request(1);
  }

  @Override
  public void onError(final Throwable throwable) {
    log("Oops I got an error from the Publisher: " + throwable.getMessage());
  }

  @Override
  public void onComplete() {
    log("Finally! I completed the subscription, I got in total " +
      totalRead + " magazines.");
  }

  private void log(final String logMessage) {
    log.info("<=========== [" + subscriberName + "] : " + logMessage);
  }

  public String getSubscriberName() {
    return subscriberName;
  }

  private void takeSomeRest() {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
