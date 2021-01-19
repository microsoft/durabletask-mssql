// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Utility for implementing semi-intelligent backoff polling for Azure queues.
    /// </summary>
    class BackoffPollingHelper
    {
        readonly RandomizedExponentialBackoffStrategy backoffStrategy;
        readonly AsyncAutoResetEvent resetEvent;

        public BackoffPollingHelper(TimeSpan minimumInterval, TimeSpan maximumInterval)
        {
            this.backoffStrategy = new RandomizedExponentialBackoffStrategy(minimumInterval, maximumInterval);
            this.resetEvent = new AsyncAutoResetEvent(signaled: false);
        }

        public void Reset()
        {
            this.resetEvent.Set();
        }

        public async Task<bool> WaitAsync(CancellationToken hostCancellationToken)
        {
            bool signaled = await this.resetEvent.WaitAsync(this.backoffStrategy.CurrentInterval, hostCancellationToken);
            this.backoffStrategy.UpdateDelay(signaled);
            return signaled;
        }

        // Borrowed from https://raw.githubusercontent.com/Azure/azure-webjobs-sdk/b798412ad74ba97cf2d85487ae8479f277bdd85c/src/Microsoft.Azure.WebJobs.Host/Timers/RandomizedExponentialBackoffStrategy.cs
        class RandomizedExponentialBackoffStrategy
        {
            public const double RandomizationFactor = 0.2;

            readonly TimeSpan minimumInterval;
            readonly TimeSpan maximumInterval;
            readonly TimeSpan deltaBackoff;

            readonly Random random;
            
            uint backoffExponent;

            public RandomizedExponentialBackoffStrategy(TimeSpan minimumInterval, TimeSpan maximumInterval)
                : this(minimumInterval, maximumInterval, minimumInterval)
            {
            }

            public RandomizedExponentialBackoffStrategy(
                TimeSpan minimumInterval,
                TimeSpan maximumInterval,
                TimeSpan deltaBackoff)
            {
                if (minimumInterval.Ticks < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(minimumInterval), "The TimeSpan must not be negative.");
                }

                if (maximumInterval.Ticks < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(maximumInterval), "The TimeSpan must not be negative.");
                }

                if (minimumInterval.Ticks > maximumInterval.Ticks)
                {
                    throw new ArgumentException(
                        $"The {nameof(minimumInterval)} must not be greater than the {nameof(maximumInterval)}.",
                        nameof(minimumInterval));
                }

                this.random = new Random();

                this.minimumInterval = minimumInterval;
                this.maximumInterval = maximumInterval;
                this.deltaBackoff = deltaBackoff;
            }

            public TimeSpan CurrentInterval { get; private set; }

            public TimeSpan UpdateDelay(bool executionSucceeded)
            {
                if (executionSucceeded)
                {
                    this.CurrentInterval = this.minimumInterval;
                    this.backoffExponent = 1;
                }
                else if (this.CurrentInterval != this.maximumInterval)
                {
                    TimeSpan backoffInterval = this.minimumInterval;

                    if (this.backoffExponent > 0)
                    {
                        double incrementMsec =
                            this.GetRandomDouble(1.0 - RandomizationFactor, 1.0 + RandomizationFactor) *
                            Math.Pow(2.0, this.backoffExponent - 1) * this.deltaBackoff.TotalMilliseconds;
                        backoffInterval += TimeSpan.FromMilliseconds(incrementMsec);
                    }

                    if (backoffInterval < this.maximumInterval)
                    {
                        this.CurrentInterval = backoffInterval;
                        this.backoffExponent++;
                    }
                    else
                    {
                        this.CurrentInterval = this.maximumInterval;
                    }
                }

                // else do nothing and keep current interval equal to max
                return this.CurrentInterval;
            }

            double GetRandomDouble(double minValue, double maxValue)
            {
                return this.random.NextDouble() * (maxValue - minValue) + minValue;
            }
        }
    }
}
