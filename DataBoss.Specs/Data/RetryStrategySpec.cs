using System;
using Cone;

namespace DataBoss.Data.Specs
{
	[Describe(typeof(RetryStrategy))]
    public class RetryStrategySpec
    {
		public void happy_path_doesnt_consider_to_retry() {
			RetryStrategy fail = (n, e) => { throw new InvalidOperationException(); };
			Check.That(() => fail.Execute(() => 7) == 7);
		}

		public void retries_while_true() {
			var maxTries = 5;
			RetryStrategy retry = (n, e) => n < maxTries;
			var attempts = 0;
			Func<int> doFail = () => {
				++attempts;
				throw new Exception();
			};
			Check.Exception<Exception>(() => retry.Execute(doFail));
			Check.That(() => attempts == maxTries);
		}

		public void propages_problem_when_not_retrying() {
			var problem = new Exception();
			RetryStrategy dontRetry = (n, error) => {
				Check.That(() => ReferenceEquals(error, problem));
				return false;
			};
			Func<int> throwProblem = () => { throw problem; };
			var e = Check.Exception<Exception>(() => dontRetry.Execute(throwProblem));
			Check.That(() => ReferenceEquals(e, problem));
		}
	}
}

